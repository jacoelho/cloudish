mod buses;
mod delivery;
mod errors;
mod patterns;
mod rules;
mod schedules;
mod scope;
mod targets;

pub use buses::{
    CreateEventBusInput, EventBusDescription, ListEventBusesInput,
    ListEventBusesOutput,
};
pub use delivery::{
    EventBridgeDeliveryDispatcher, EventBridgePlannedDelivery,
    NoopEventBridgeDeliveryDispatcher, PutEventsInput, PutEventsOutput,
    PutEventsRequestEntry, PutEventsResultEntry,
};
pub use errors::EventBridgeError;
pub use rules::{
    DescribeRuleOutput, EventBridgeRuleState, ListRulesInput, ListRulesOutput,
    PutRuleInput, PutRuleOutput, RuleSummary,
};
pub use scope::EventBridgeScope;
pub use targets::{
    EventBridgeInputTransformer, EventBridgeTarget, ListTargetsByRuleInput,
    ListTargetsByRuleOutput, PutTargetsFailureEntry, PutTargetsInput,
    PutTargetsOutput, RemoveTargetsFailureEntry, RemoveTargetsInput,
    RemoveTargetsOutput,
};

use crate::buses::{
    DEFAULT_EVENT_BUS_NAME, EventBusKey, EventBusRecord, bus_description,
    decode_next_token, default_event_bus, normalize_bus_name, normalize_limit,
    validate_create_event_bus, validate_delete_event_bus_name,
};
use crate::delivery::{
    ValidatedPutEventsEntry, default_time, dispatcher, event_payload,
    put_events_failure, put_events_output, put_events_success,
    rule_arn as delivery_rule_arn, scheduled_event_payload,
    validate_put_events_entry, validate_put_events_input,
};
use crate::errors::storage_error;
use crate::patterns::event_matches;
use crate::rules::{
    EventBridgeRuleKey, StoredRule, describe_rule_output,
    normalize_rule_bus_name, normalize_rule_limit, rule_summary,
    validate_put_rule, validate_rule_name,
};
use crate::targets::{
    normalize_target_limit, put_targets_failure, put_targets_output,
    remove_targets_output, render_target_payload, validate_put_targets_input,
    validate_remove_targets_input, validate_target_scope,
};
use aws::{BackgroundScheduler, Clock, ScheduledTaskHandle};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use storage::{StorageFactory, StorageHandle};

#[derive(Clone)]
pub struct EventBridgeService {
    bus_store: StorageHandle<EventBusKey, EventBusRecord>,
    clock: Arc<dyn Clock>,
    dispatcher: Arc<dyn EventBridgeDeliveryDispatcher>,
    next_event_id: Arc<AtomicU64>,
    mutation_lock: Arc<Mutex<()>>,
    rule_store: StorageHandle<EventBridgeRuleKey, StoredRule>,
    scheduled_rules:
        Arc<Mutex<BTreeMap<String, Box<dyn ScheduledTaskHandle>>>>,
    scheduler: Arc<dyn BackgroundScheduler>,
}

#[derive(Clone)]
pub struct EventBridgeServiceDependencies {
    pub clock: Arc<dyn Clock>,
    pub dispatcher: Arc<dyn EventBridgeDeliveryDispatcher>,
    pub scheduler: Arc<dyn BackgroundScheduler>,
}

impl EventBridgeService {
    /// Creates a storage-backed `EventBridge` service.
    pub fn new(
        factory: &StorageFactory,
        dependencies: EventBridgeServiceDependencies,
    ) -> Self {
        Self {
            bus_store: factory.create("eventbridge", "buses"),
            clock: dependencies.clock,
            dispatcher: dispatcher(dependencies.dispatcher),
            next_event_id: Arc::new(AtomicU64::new(0)),
            mutation_lock: Arc::new(Mutex::new(())),
            rule_store: factory.create("eventbridge", "rules"),
            scheduled_rules: Arc::default(),
            scheduler: dependencies.scheduler,
        }
    }

    /// Restores persisted scheduled rules into the configured background
    /// scheduler.
    ///
    /// # Errors
    ///
    /// Returns an error when persisted scheduled rules cannot be restored.
    pub fn restore_schedules(&self) -> Result<(), EventBridgeError> {
        for key in self.rule_store.keys() {
            let Some(rule) = self.rule_store.get(&key) else {
                continue;
            };
            self.sync_rule_schedule(&key, &rule)?;
        }

        Ok(())
    }

    /// Cancels any active scheduled-rule handles owned by this service.
    ///
    /// # Errors
    ///
    /// Returns an error when a scheduled-rule handle cannot be cancelled.
    pub fn shutdown(&self) -> Result<(), EventBridgeError> {
        let rule_arns = self
            .scheduled_rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for rule_arn in rule_arns {
            self.cancel_rule_schedule(&rule_arn)?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, duplicate, or persistence errors when the custom
    /// event bus request is invalid or cannot be stored.
    pub fn create_event_bus(
        &self,
        scope: &EventBridgeScope,
        input: CreateEventBusInput,
    ) -> Result<EventBusDescription, EventBridgeError> {
        validate_create_event_bus(&input)?;

        let _guard = self.lock_state();
        let key = EventBusKey::new(scope, &input.name)?;
        if self.bus_store.get(&key).is_some() {
            return Err(EventBridgeError::ResourceAlreadyExists {
                message: format!("Event bus {} already exists.", input.name),
            });
        }

        self.bus_store.put(key, EventBusRecord).map_err(|source| {
            storage_error("writing event bus state", source)
        })?;

        Ok(bus_description(scope, &input.name))
    }

    /// # Errors
    ///
    /// Returns validation, not-found, in-use, or persistence errors when the
    /// event bus cannot be deleted.
    pub fn delete_event_bus(
        &self,
        scope: &EventBridgeScope,
        name: &str,
    ) -> Result<(), EventBridgeError> {
        validate_delete_event_bus_name(name)?;

        let _guard = self.lock_state();
        let key = EventBusKey::new(scope, name)?;
        if self.bus_store.get(&key).is_none() {
            return Err(EventBridgeError::ResourceNotFound {
                message: format!("Event bus {name} does not exist."),
            });
        }
        if self.rule_store.keys().into_iter().any(|candidate| {
            candidate.account_id == *scope.account_id()
                && candidate.region == *scope.region()
                && candidate.bus_name == name
        }) {
            return Err(EventBridgeError::Validation {
                message: format!(
                    "Event bus {name} cannot be deleted while it still has rules."
                ),
            });
        }

        self.bus_store.delete(&key).map_err(|source| {
            storage_error("deleting event bus state", source)
        })
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the named bus cannot be
    /// resolved.
    pub fn describe_event_bus(
        &self,
        scope: &EventBridgeScope,
        event_bus_name: Option<&str>,
    ) -> Result<EventBusDescription, EventBridgeError> {
        let name = normalize_bus_name(event_bus_name)?;
        if name == DEFAULT_EVENT_BUS_NAME {
            return Ok(default_event_bus(scope));
        }

        let key = EventBusKey::new(scope, &name)?;
        if self.bus_store.get(&key).is_none() {
            return Err(EventBridgeError::ResourceNotFound {
                message: format!("Event bus {name} does not exist."),
            });
        }

        Ok(bus_description(scope, &name))
    }

    /// # Errors
    ///
    /// Returns validation errors when the paging token or page size is
    /// invalid.
    pub fn list_event_buses(
        &self,
        scope: &EventBridgeScope,
        input: ListEventBusesInput,
    ) -> Result<ListEventBusesOutput, EventBridgeError> {
        let mut buses = vec![default_event_bus(scope)];
        buses.extend(
            self.bus_store
                .keys()
                .into_iter()
                .filter(|candidate| {
                    candidate.account_id == *scope.account_id()
                        && candidate.region == *scope.region()
                })
                .map(|candidate| bus_description(scope, &candidate.name)),
        );
        buses.sort_by(|left, right| left.name.cmp(&right.name));

        let prefix = input.name_prefix.unwrap_or_default();
        let filtered = buses
            .into_iter()
            .filter(|bus| bus.name.starts_with(&prefix))
            .collect::<Vec<_>>();
        let start = decode_next_token(input.next_token.as_deref())?;
        let limit = normalize_limit(input.limit, "Limit")?;
        let (event_buses, next_token) = page_items(&filtered, start, limit)?;

        Ok(ListEventBusesOutput { event_buses, next_token })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the rule
    /// request cannot be applied.
    pub fn put_rule(
        &self,
        scope: &EventBridgeScope,
        input: PutRuleInput,
    ) -> Result<PutRuleOutput, EventBridgeError> {
        let event_bus_name =
            normalize_rule_bus_name(input.event_bus_name.as_deref())?;
        let validated = validate_put_rule(&input, &event_bus_name)?;
        self.ensure_bus_exists(scope, &event_bus_name)?;

        let _guard = self.lock_state();
        let key =
            EventBridgeRuleKey::new(scope, &event_bus_name, &input.name)?;
        let existing = self.rule_store.get(&key);
        let stored = StoredRule {
            description: validated.description,
            event_pattern: validated.event_pattern,
            event_pattern_source: validated.event_pattern_source,
            role_arn: validated.role_arn,
            schedule_expression: validated.schedule_expression,
            schedule_expression_source: validated.schedule_expression_source,
            state: validated.state,
            targets: existing.map(|rule| rule.targets).unwrap_or_default(),
        };
        self.persist_rule_with_schedule(&key, stored)?;

        Ok(PutRuleOutput { rule_arn: key.rule_arn() })
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the named rule cannot be
    /// resolved.
    pub fn describe_rule(
        &self,
        scope: &EventBridgeScope,
        name: &str,
        event_bus_name: Option<&str>,
    ) -> Result<DescribeRuleOutput, EventBridgeError> {
        let key = self.rule_key(scope, event_bus_name, name)?;
        let rule = self.load_rule(&key)?;

        Ok(describe_rule_output(&key, &rule))
    }

    /// # Errors
    ///
    /// Returns validation errors when the page size or token is invalid.
    pub fn list_rules(
        &self,
        scope: &EventBridgeScope,
        input: ListRulesInput,
    ) -> Result<ListRulesOutput, EventBridgeError> {
        let bus_name =
            normalize_rule_bus_name(input.event_bus_name.as_deref())?;
        self.ensure_bus_exists(scope, &bus_name)?;
        let prefix = input.name_prefix.unwrap_or_default();
        let mut rules = self
            .rule_store
            .keys()
            .into_iter()
            .filter(|candidate| {
                candidate.account_id == *scope.account_id()
                    && candidate.region == *scope.region()
                    && candidate.bus_name == bus_name
                    && candidate.name.starts_with(&prefix)
            })
            .filter_map(|candidate| {
                self.rule_store
                    .get(&candidate)
                    .map(|rule| rule_summary(&candidate, &rule))
            })
            .collect::<Vec<_>>();
        rules.sort_by(|left, right| left.name.cmp(&right.name));

        let start = decode_next_token(input.next_token.as_deref())?;
        let limit = normalize_rule_limit(input.limit)?;
        let (rules, next_token) = page_items(&rules, start, limit)?;

        Ok(ListRulesOutput { next_token, rules })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the named
    /// rule cannot be disabled.
    pub fn disable_rule(
        &self,
        scope: &EventBridgeScope,
        name: &str,
        event_bus_name: Option<&str>,
    ) -> Result<(), EventBridgeError> {
        self.set_rule_state(
            scope,
            name,
            event_bus_name,
            EventBridgeRuleState::Disabled,
        )
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the named
    /// rule cannot be enabled.
    pub fn enable_rule(
        &self,
        scope: &EventBridgeScope,
        name: &str,
        event_bus_name: Option<&str>,
    ) -> Result<(), EventBridgeError> {
        self.set_rule_state(
            scope,
            name,
            event_bus_name,
            EventBridgeRuleState::Enabled,
        )
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the rule
    /// cannot be removed.
    pub fn delete_rule(
        &self,
        scope: &EventBridgeScope,
        name: &str,
        event_bus_name: Option<&str>,
    ) -> Result<(), EventBridgeError> {
        let key = self.rule_key(scope, event_bus_name, name)?;
        let rule = self.load_rule(&key)?;
        if !rule.targets.is_empty() {
            return Err(EventBridgeError::Validation {
                message: format!(
                    "Rule {name} cannot be deleted while it still has targets."
                ),
            });
        }

        self.cancel_rule_schedule(&key.rule_arn().to_string())?;
        self.rule_store
            .delete(&key)
            .map_err(|source| storage_error("deleting rule state", source))
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the target
    /// request cannot be applied.
    pub fn put_targets(
        &self,
        scope: &EventBridgeScope,
        input: PutTargetsInput,
    ) -> Result<PutTargetsOutput, EventBridgeError> {
        validate_put_targets_input(&input)?;
        let key = self.rule_key(
            scope,
            input.event_bus_name.as_deref(),
            &input.rule,
        )?;
        let mut rule = self.load_rule(&key)?;
        let rule_arn = key.rule_arn();
        let mut failed_entries = Vec::new();

        for target in input.targets {
            if let Err(error) = validate_target_scope(&rule_arn, &target) {
                failed_entries.push(put_targets_failure(&target.id, &error));
                continue;
            }
            if let Err(error) =
                self.dispatcher.validate_target(scope, &rule_arn, &target)
            {
                failed_entries.push(put_targets_failure(&target.id, &error));
                continue;
            }

            rule.targets.insert(target.id.clone(), target);
        }

        self.rule_store
            .put(key, rule)
            .map_err(|source| storage_error("writing target state", source))?;

        Ok(put_targets_output(failed_entries))
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or paging errors when the request
    /// cannot be satisfied.
    pub fn list_targets_by_rule(
        &self,
        scope: &EventBridgeScope,
        input: ListTargetsByRuleInput,
    ) -> Result<ListTargetsByRuleOutput, EventBridgeError> {
        let key = self.rule_key(
            scope,
            input.event_bus_name.as_deref(),
            &input.rule,
        )?;
        let mut targets =
            self.load_rule(&key)?.targets.into_values().collect::<Vec<_>>();
        targets.sort_by(|left, right| left.id.cmp(&right.id));

        let start = decode_next_token(input.next_token.as_deref())?;
        let limit = normalize_target_limit(input.limit)?;
        let (targets, next_token) = page_items(&targets, start, limit)?;

        Ok(ListTargetsByRuleOutput { next_token, targets })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when target
    /// removal cannot be recorded.
    pub fn remove_targets(
        &self,
        scope: &EventBridgeScope,
        input: RemoveTargetsInput,
    ) -> Result<RemoveTargetsOutput, EventBridgeError> {
        validate_remove_targets_input(&input)?;
        let key = self.rule_key(
            scope,
            input.event_bus_name.as_deref(),
            &input.rule,
        )?;
        let mut rule = self.load_rule(&key)?;

        for id in input.ids {
            let _ = rule.targets.remove(&id);
        }

        self.rule_store
            .put(key, rule)
            .map_err(|source| storage_error("writing target state", source))?;

        Ok(remove_targets_output())
    }

    /// # Errors
    ///
    /// Returns request-level validation errors when the batch cannot be
    /// accepted.
    pub fn put_events(
        &self,
        scope: &EventBridgeScope,
        input: PutEventsInput,
    ) -> Result<PutEventsOutput, EventBridgeError> {
        validate_put_events_input(&input)?;
        let timestamp = default_time(self.clock.now());
        let mut entries = Vec::with_capacity(input.entries.len());

        for entry in input.entries {
            let validated = match validate_put_events_entry(entry) {
                Ok(validated) => validated,
                Err(error) => {
                    entries.push(error);
                    continue;
                }
            };
            let event_bus_name = match normalize_bus_name(
                validated.event_bus_name.as_deref(),
            ) {
                Ok(name) => name,
                Err(error) => {
                    entries.push(result_entry_for_error(error));
                    continue;
                }
            };
            if !self.bus_exists(scope, &event_bus_name) {
                entries.push(put_events_failure(
                    "ResourceNotFoundException",
                    &format!("Event bus {event_bus_name} does not exist."),
                ));
                continue;
            }

            let event_id = self.next_event_id();
            let event = event_payload(
                scope,
                &event_id,
                &event_bus_name,
                &validated,
                &timestamp,
            );
            let deliveries = self.deliveries_for_entry(
                scope,
                &event_bus_name,
                &validated,
                &event,
            )?;
            match self.dispatcher.dispatch(deliveries) {
                Ok(()) => entries.push(put_events_success(event_id)),
                Err(error) => entries.push(put_events_failure(
                    "InternalException",
                    &error.to_string(),
                )),
            }
        }

        Ok(put_events_output(entries))
    }

    fn deliveries_for_entry(
        &self,
        scope: &EventBridgeScope,
        event_bus_name: &str,
        validated: &ValidatedPutEventsEntry,
        event: &serde_json::Value,
    ) -> Result<Vec<EventBridgePlannedDelivery>, EventBridgeError> {
        let mut deliveries = Vec::new();

        for key in self.rule_store.keys().into_iter().filter(|candidate| {
            candidate.account_id == *scope.account_id()
                && candidate.region == *scope.region()
                && candidate.bus_name == event_bus_name
        }) {
            let Some(rule) = self.rule_store.get(&key) else {
                continue;
            };
            if !rule.state.is_enabled() {
                continue;
            }
            let Some(pattern) = rule.event_pattern.as_ref() else {
                continue;
            };
            if !event_matches(
                pattern,
                &validated.source,
                &validated.detail_type,
                &validated.detail,
            ) {
                continue;
            }

            deliveries.extend(self.rule_deliveries(
                &key,
                &rule.targets,
                event,
            )?);
        }

        Ok(deliveries)
    }

    fn rule_deliveries(
        &self,
        key: &EventBridgeRuleKey,
        targets: &BTreeMap<String, EventBridgeTarget>,
        event: &serde_json::Value,
    ) -> Result<Vec<EventBridgePlannedDelivery>, EventBridgeError> {
        targets
            .values()
            .cloned()
            .map(|target| {
                validate_target_scope(&key.rule_arn(), &target)?;
                Ok(EventBridgePlannedDelivery {
                    payload: render_target_payload(&target, event)?,
                    rule_arn: key.rule_arn(),
                    target,
                })
            })
            .collect()
    }

    fn set_rule_state(
        &self,
        scope: &EventBridgeScope,
        name: &str,
        event_bus_name: Option<&str>,
        state: EventBridgeRuleState,
    ) -> Result<(), EventBridgeError> {
        let key = self.rule_key(scope, event_bus_name, name)?;
        let mut rule = self.load_rule(&key)?;
        rule.state = state;
        self.persist_rule_with_schedule(&key, rule)
    }

    fn rule_key(
        &self,
        scope: &EventBridgeScope,
        event_bus_name: Option<&str>,
        name: &str,
    ) -> Result<EventBridgeRuleKey, EventBridgeError> {
        validate_rule_name(name)?;
        let bus_name = normalize_rule_bus_name(event_bus_name)?;
        self.ensure_bus_exists(scope, &bus_name)?;
        EventBridgeRuleKey::new(scope, &bus_name, name)
    }

    fn ensure_bus_exists(
        &self,
        scope: &EventBridgeScope,
        event_bus_name: &str,
    ) -> Result<(), EventBridgeError> {
        if self.bus_exists(scope, event_bus_name) {
            return Ok(());
        }

        Err(EventBridgeError::ResourceNotFound {
            message: format!("Event bus {event_bus_name} does not exist."),
        })
    }

    fn bus_exists(
        &self,
        scope: &EventBridgeScope,
        event_bus_name: &str,
    ) -> bool {
        if event_bus_name == DEFAULT_EVENT_BUS_NAME {
            return true;
        }

        EventBusKey::new(scope, event_bus_name)
            .ok()
            .and_then(|key| self.bus_store.get(&key))
            .is_some()
    }

    fn load_rule(
        &self,
        key: &EventBridgeRuleKey,
    ) -> Result<StoredRule, EventBridgeError> {
        self.rule_store.get(key).ok_or_else(|| {
            EventBridgeError::ResourceNotFound {
                message: format!("Rule {} does not exist.", key.name),
            }
        })
    }

    fn sync_rule_schedule(
        &self,
        key: &EventBridgeRuleKey,
        rule: &StoredRule,
    ) -> Result<(), EventBridgeError> {
        let rule_arn = key.rule_arn().to_string();
        let handle = self.stage_rule_schedule(key, rule)?;
        self.replace_rule_schedule(&rule_arn, handle)
    }

    fn persist_rule_with_schedule(
        &self,
        key: &EventBridgeRuleKey,
        rule: StoredRule,
    ) -> Result<(), EventBridgeError> {
        let rule_arn = key.rule_arn().to_string();
        let staged_handle = self.stage_rule_schedule(key, &rule)?;
        if let Err(error) = self
            .rule_store
            .put(key.clone(), rule)
            .map_err(|source| storage_error("writing rule state", source))
        {
            if let Some(handle) = staged_handle {
                let _ = handle.cancel();
            }
            return Err(error);
        }

        self.replace_rule_schedule(&rule_arn, staged_handle)
    }

    fn stage_rule_schedule(
        &self,
        key: &EventBridgeRuleKey,
        rule: &StoredRule,
    ) -> Result<Option<Box<dyn ScheduledTaskHandle>>, EventBridgeError> {
        let Some(schedule) = rule.schedule_expression.as_ref() else {
            return Ok(None);
        };
        if !rule.state.is_enabled() || key.bus_name != DEFAULT_EVENT_BUS_NAME {
            return Ok(None);
        }

        let service = self.clone();
        let scope = key.scope();
        let bus_name = key.bus_name.clone();
        let rule_name = key.name.clone();
        let scheduled_rule_name = rule_name.clone();
        self.scheduler
            .schedule_repeating(
                format!("eventbridge-rule-{bus_name}-{rule_name}"),
                schedule.interval(),
                Arc::new(move || {
                    let _ = service.dispatch_scheduled_rule(
                        &scope,
                        &bus_name,
                        &scheduled_rule_name,
                    );
                }),
            )
            .map(Some)
            .map_err(|source| EventBridgeError::InternalFailure {
                message: format!(
                    "Failed to schedule EventBridge rule {rule_name}: {source}"
                ),
            })
    }

    fn replace_rule_schedule(
        &self,
        rule_arn: &str,
        handle: Option<Box<dyn ScheduledTaskHandle>>,
    ) -> Result<(), EventBridgeError> {
        let previous = {
            let mut scheduled_rules = self
                .scheduled_rules
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match handle {
                Some(handle) => {
                    scheduled_rules.insert(rule_arn.to_owned(), handle)
                }
                None => scheduled_rules.remove(rule_arn),
            }
        };
        if let Some(handle) = previous {
            handle.cancel().map_err(|source| EventBridgeError::InternalFailure {
                message: format!(
                    "Failed to cancel EventBridge rule schedule {rule_arn}: {source}"
                ),
            })?;
        }

        Ok(())
    }

    fn dispatch_scheduled_rule(
        &self,
        scope: &EventBridgeScope,
        bus_name: &str,
        rule_name: &str,
    ) -> Result<(), EventBridgeError> {
        let key = EventBridgeRuleKey::new(scope, bus_name, rule_name)?;
        let rule = self.load_rule(&key)?;
        if !rule.state.is_enabled() {
            return Ok(());
        }
        let timestamp = default_time(self.clock.now());
        let event = scheduled_event_payload(
            scope,
            &self.next_event_id(),
            &delivery_rule_arn(scope, bus_name, rule_name),
            &timestamp,
        );
        self.dispatcher.dispatch(self.rule_deliveries(
            &key,
            &rule.targets,
            &event,
        )?)
    }

    fn cancel_rule_schedule(
        &self,
        rule_arn: &str,
    ) -> Result<(), EventBridgeError> {
        let handle = self
            .scheduled_rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(rule_arn);
        if let Some(handle) = handle {
            handle.cancel().map_err(|source| EventBridgeError::InternalFailure {
                message: format!(
                    "Failed to cancel EventBridge rule schedule {rule_arn}: {source}"
                ),
            })?;
        }

        Ok(())
    }

    fn next_event_id(&self) -> String {
        let id = self.next_event_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("00000000-0000-0000-0000-{id:012}")
    }

    fn lock_state(&self) -> MutexGuard<'_, ()> {
        self.mutation_lock.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn page_items<T: Clone>(
    items: &[T],
    start: usize,
    limit: usize,
) -> Result<(Vec<T>, Option<String>), EventBridgeError> {
    if start > items.len() {
        return Err(EventBridgeError::Validation {
            message: "NextToken is invalid.".to_owned(),
        });
    }

    let end = (start + limit).min(items.len());
    let next_token = (end < items.len()).then(|| end.to_string());
    let page =
        items.get(start..end).ok_or_else(|| EventBridgeError::Validation {
            message: "NextToken is invalid.".to_owned(),
        })?;

    Ok((page.to_vec(), next_token))
}

fn result_entry_for_error(error: EventBridgeError) -> PutEventsResultEntry {
    match error {
        EventBridgeError::ConcurrentModification { message } => {
            put_events_failure("ConcurrentModificationException", &message)
        }
        EventBridgeError::InternalFailure { message } => {
            put_events_failure("InternalException", &message)
        }
        EventBridgeError::ResourceAlreadyExists { message } => {
            put_events_failure("ResourceAlreadyExistsException", &message)
        }
        EventBridgeError::ResourceNotFound { message } => {
            put_events_failure("ResourceNotFoundException", &message)
        }
        EventBridgeError::UnsupportedOperation { message } => {
            put_events_failure("UnsupportedOperationException", &message)
        }
        EventBridgeError::Validation { message } => {
            put_events_failure("ValidationException", &message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateEventBusInput, EventBridgeDeliveryDispatcher, EventBridgeError,
        EventBridgePlannedDelivery, EventBridgeScope, EventBridgeService,
        EventBridgeServiceDependencies, EventBridgeTarget,
        ListEventBusesInput, ListRulesInput, ListTargetsByRuleInput,
        PutEventsInput, PutEventsRequestEntry, PutRuleInput, PutTargetsInput,
        RemoveTargetsInput,
    };
    use aws::{
        BackgroundScheduler, Clock, InfrastructureError, ScheduledTaskHandle,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, Default)]
    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            UNIX_EPOCH
        }
    }

    #[derive(Default)]
    struct RecordingDispatcher {
        deliveries: Mutex<Vec<EventBridgePlannedDelivery>>,
        validated_targets: Mutex<Vec<String>>,
    }

    impl RecordingDispatcher {
        fn delivery_count(&self) -> usize {
            self.deliveries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len()
        }

        fn run_payloads(&self) -> Vec<String> {
            self.deliveries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|delivery| {
                    String::from_utf8(delivery.payload.clone())
                        .expect("payload should be utf-8")
                })
                .collect()
        }
    }

    impl EventBridgeDeliveryDispatcher for RecordingDispatcher {
        fn validate_target(
            &self,
            _scope: &EventBridgeScope,
            _rule_arn: &aws::Arn,
            target: &EventBridgeTarget,
        ) -> Result<(), EventBridgeError> {
            self.validated_targets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(target.id.clone());
            Ok(())
        }

        fn dispatch(
            &self,
            deliveries: Vec<EventBridgePlannedDelivery>,
        ) -> Result<(), EventBridgeError> {
            self.deliveries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(deliveries);
            Ok(())
        }
    }

    #[derive(Default)]
    struct ManualScheduler {
        tasks: Mutex<Vec<ScheduledTask>>,
    }

    impl ManualScheduler {
        fn active_task_count(&self) -> usize {
            self.tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .filter(|task| task.active.load(Ordering::Relaxed))
                .count()
        }

        fn run_pending(&self) {
            let tasks = self
                .tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            for task in tasks {
                if task.active.load(Ordering::Relaxed) {
                    (task.run)();
                }
            }
        }
    }

    impl BackgroundScheduler for ManualScheduler {
        fn schedule_repeating(
            &self,
            _task_name: String,
            _interval: Duration,
            task: Arc<dyn Fn() + Send + Sync>,
        ) -> Result<Box<dyn ScheduledTaskHandle>, InfrastructureError>
        {
            let active = Arc::new(AtomicBool::new(true));
            self.tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(ScheduledTask { active: active.clone(), run: task });
            Ok(Box::new(NoopHandle { active }))
        }
    }

    #[derive(Clone)]
    struct ScheduledTask {
        active: Arc<AtomicBool>,
        run: Arc<dyn Fn() + Send + Sync>,
    }

    struct NoopHandle {
        active: Arc<AtomicBool>,
    }

    impl ScheduledTaskHandle for NoopHandle {
        fn cancel(&self) -> Result<(), InfrastructureError> {
            self.active.store(false, Ordering::Relaxed);
            Ok(())
        }
    }

    fn scope() -> EventBridgeScope {
        EventBridgeScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn factory(label: &str) -> StorageFactory {
        StorageFactory::new(StorageConfig::new(
            std::env::temp_dir().join(format!("eventbridge-{label}")),
            StorageMode::Memory,
        ))
    }

    fn persistent_factory(path: &std::path::Path) -> StorageFactory {
        std::fs::create_dir_all(path)
            .expect("persistent EventBridge state directory should exist");
        StorageFactory::new(StorageConfig::new(path, StorageMode::Persistent))
    }

    fn unique_label(prefix: &str) -> String {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }

    fn service(
        label: &str,
    ) -> (EventBridgeService, Arc<RecordingDispatcher>, Arc<ManualScheduler>)
    {
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler = Arc::new(ManualScheduler::default());
        let service = EventBridgeService::new(
            &factory(label),
            EventBridgeServiceDependencies {
                clock: Arc::new(FixedClock),
                dispatcher: dispatcher.clone(),
                scheduler: scheduler.clone(),
            },
        );

        (service, dispatcher, scheduler)
    }

    fn lambda_target(id: &str) -> EventBridgeTarget {
        EventBridgeTarget {
            arn: "arn:aws:lambda:eu-west-2:000000000000:function:processor"
                .parse()
                .expect("lambda arn should parse"),
            id: id.to_owned(),
            input: None,
            input_path: None,
            input_transformer: None,
            role_arn: None,
        }
    }

    #[test]
    fn event_buses_rules_targets_and_put_events_round_trip() {
        let (service, dispatcher, _) = service("round-trip");
        let scope = scope();

        let created = service
            .create_event_bus(
                &scope,
                CreateEventBusInput { name: "orders".to_owned() },
            )
            .expect("event bus should create");
        assert_eq!(created.name, "orders");

        service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: Some("orders rule".to_owned()),
                    event_bus_name: Some("orders".to_owned()),
                    event_pattern: Some(
                        "{\"source\":[\"orders\"],\"detail-type\":[\"Created\"],\"detail\":{\"state\":[\"queued\"]}}"
                            .to_owned(),
                    ),
                    name: "queue-orders".to_owned(),
                    role_arn: None,
                    schedule_expression: None,
                    state: None,
                },
            )
            .expect("rule should create");
        service
            .put_targets(
                &scope,
                PutTargetsInput {
                    event_bus_name: Some("orders".to_owned()),
                    rule: "queue-orders".to_owned(),
                    targets: vec![lambda_target("processor")],
                },
            )
            .expect("targets should store");

        let listed_buses = service
            .list_event_buses(
                &scope,
                ListEventBusesInput {
                    limit: Some(10),
                    name_prefix: Some("o".to_owned()),
                    next_token: None,
                },
            )
            .expect("buses should list");
        assert_eq!(listed_buses.event_buses.len(), 1);
        let listed_rules = service
            .list_rules(
                &scope,
                ListRulesInput {
                    event_bus_name: Some("orders".to_owned()),
                    limit: Some(10),
                    name_prefix: Some("queue".to_owned()),
                    next_token: None,
                },
            )
            .expect("rules should list");
        assert_eq!(listed_rules.rules.len(), 1);
        let listed_targets = service
            .list_targets_by_rule(
                &scope,
                ListTargetsByRuleInput {
                    event_bus_name: Some("orders".to_owned()),
                    limit: Some(10),
                    next_token: None,
                    rule: "queue-orders".to_owned(),
                },
            )
            .expect("targets should list");
        assert_eq!(listed_targets.targets.len(), 1);

        let output = service
            .put_events(
                &scope,
                PutEventsInput {
                    entries: vec![PutEventsRequestEntry {
                        detail: "{\"state\":\"queued\"}".to_owned(),
                        detail_type: "Created".to_owned(),
                        event_bus_name: Some("orders".to_owned()),
                        resources: Vec::new(),
                        source: "orders".to_owned(),
                        time: None,
                        trace_header: None,
                    }],
                },
            )
            .expect("events should publish");

        assert_eq!(output.failed_entry_count, 0);
        assert_eq!(dispatcher.delivery_count(), 1);
    }

    #[test]
    fn put_events_preserves_partial_failure_order() {
        let (service, _, _) = service("partial");
        let scope = scope();

        let output = service
            .put_events(
                &scope,
                PutEventsInput {
                    entries: vec![
                        PutEventsRequestEntry {
                            detail: "{\"state\":\"queued\"}".to_owned(),
                            detail_type: "Created".to_owned(),
                            event_bus_name: Some("missing".to_owned()),
                            resources: Vec::new(),
                            source: "orders".to_owned(),
                            time: None,
                            trace_header: None,
                        },
                        PutEventsRequestEntry {
                            detail: "{\"state\":\"queued\"}".to_owned(),
                            detail_type: "Created".to_owned(),
                            event_bus_name: None,
                            resources: Vec::new(),
                            source: "orders".to_owned(),
                            time: None,
                            trace_header: None,
                        },
                    ],
                },
            )
            .expect("request should return partial results");

        assert_eq!(output.failed_entry_count, 1);
        assert!(output.entries[0].event_id.is_none());
        assert!(output.entries[1].event_id.is_some());
    }

    #[test]
    fn put_targets_rejects_cross_account_and_cross_region_target_arns() {
        let (service, _, _) = service("target-scope");
        let scope = scope();

        service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: Some(
                        "{\"source\":[\"orders\"]}".to_owned(),
                    ),
                    name: "orders-created".to_owned(),
                    role_arn: None,
                    schedule_expression: None,
                    state: None,
                },
            )
            .expect("rule should create");

        let output = service
            .put_targets(
                &scope,
                PutTargetsInput {
                    event_bus_name: None,
                    rule: "orders-created".to_owned(),
                    targets: vec![
                        EventBridgeTarget {
                            arn: "arn:aws:sqs:us-east-1:000000000000:orders"
                                .parse()
                                .expect("SQS target ARN should parse"),
                            id: "cross-region".to_owned(),
                            input: None,
                            input_path: None,
                            input_transformer: None,
                            role_arn: None,
                        },
                        EventBridgeTarget {
                            arn: "arn:aws:lambda:eu-west-2:111111111111:function:processor"
                                .parse()
                                .expect("Lambda target ARN should parse"),
                            id: "cross-account".to_owned(),
                            input: None,
                            input_path: None,
                            input_transformer: None,
                            role_arn: None,
                        },
                    ],
                },
            )
            .expect("put targets should return failure entries");

        assert_eq!(output.failed_entry_count, 2);
        assert!(output.failed_entries.iter().all(|entry| {
            entry.error_code == "ValidationException"
                && entry
                    .error_message
                    .contains("must match the rule account and region")
        }));
        assert!(
            service
                .list_targets_by_rule(
                    &scope,
                    ListTargetsByRuleInput {
                        event_bus_name: None,
                        limit: Some(10),
                        next_token: None,
                        rule: "orders-created".to_owned(),
                    },
                )
                .expect("targets should list")
                .targets
                .is_empty()
        );
    }

    #[test]
    fn put_events_rejects_persisted_cross_scope_targets_before_dispatch() {
        let (service, dispatcher, _) = service("persisted-target-scope");
        let scope = scope();

        service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: Some(
                        "{\"source\":[\"orders\"]}".to_owned(),
                    ),
                    name: "orders-created".to_owned(),
                    role_arn: None,
                    schedule_expression: None,
                    state: None,
                },
            )
            .expect("rule should create");

        let key = super::EventBridgeRuleKey::new(
            &scope,
            super::DEFAULT_EVENT_BUS_NAME,
            "orders-created",
        )
        .expect("rule key should build");
        let mut stored =
            service.rule_store.get(&key).expect("stored rule should exist");
        stored.targets.insert(
            "cross-region".to_owned(),
            EventBridgeTarget {
                arn: "arn:aws:sqs:us-east-1:000000000000:orders"
                    .parse()
                    .expect("SQS target ARN should parse"),
                id: "cross-region".to_owned(),
                input: None,
                input_path: None,
                input_transformer: None,
                role_arn: None,
            },
        );
        service
            .rule_store
            .put(key, stored)
            .expect("persisted rule should update");

        let error = service
            .put_events(
                &scope,
                PutEventsInput {
                    entries: vec![PutEventsRequestEntry {
                        detail: "{\"kind\":\"queued\"}".to_owned(),
                        detail_type: "Created".to_owned(),
                        event_bus_name: None,
                        resources: Vec::new(),
                        source: "orders".to_owned(),
                        time: None,
                        trace_header: None,
                    }],
                },
            )
            .expect_err("persisted mismatched target should fail");

        assert!(matches!(
            error,
            EventBridgeError::Validation { ref message }
                if message.contains("must match the rule account and region")
        ));
        assert_eq!(dispatcher.delivery_count(), 0);
    }

    #[test]
    fn scheduled_default_bus_rules_dispatch_through_scheduler() {
        let (service, dispatcher, scheduler) = service("schedule");
        let scope = scope();

        service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: None,
                    name: "nightly".to_owned(),
                    role_arn: None,
                    schedule_expression: Some("rate(1 minute)".to_owned()),
                    state: None,
                },
            )
            .expect("scheduled rule should create");
        service
            .put_targets(
                &scope,
                PutTargetsInput {
                    event_bus_name: None,
                    rule: "nightly".to_owned(),
                    targets: vec![EventBridgeTarget {
                        input: Some("{\"kind\":\"scheduled\"}".to_owned()),
                        ..lambda_target("processor")
                    }],
                },
            )
            .expect("target should store");

        scheduler.run_pending();

        assert_eq!(dispatcher.delivery_count(), 1);
        assert_eq!(
            dispatcher.run_payloads(),
            vec!["{\"kind\":\"scheduled\"}"]
        );
    }

    #[test]
    fn restore_schedules_replays_persisted_rules_without_duplicate_tasks() {
        let root =
            std::env::temp_dir().join(unique_label("eventbridge-restore"));
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let first_scheduler = Arc::new(ManualScheduler::default());
        let first_service = EventBridgeService::new(
            &persistent_factory(&root),
            EventBridgeServiceDependencies {
                clock: Arc::new(FixedClock),
                dispatcher,
                scheduler: first_scheduler,
            },
        );
        let scope = scope();

        first_service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: None,
                    name: "nightly".to_owned(),
                    role_arn: None,
                    schedule_expression: Some("rate(1 minute)".to_owned()),
                    state: None,
                },
            )
            .expect("scheduled rule should create");
        first_service
            .put_targets(
                &scope,
                PutTargetsInput {
                    event_bus_name: None,
                    rule: "nightly".to_owned(),
                    targets: vec![EventBridgeTarget {
                        input: Some("{\"kind\":\"scheduled\"}".to_owned()),
                        ..lambda_target("processor")
                    }],
                },
            )
            .expect("target should store");

        let restored_dispatcher = Arc::new(RecordingDispatcher::default());
        let restored_scheduler = Arc::new(ManualScheduler::default());
        let restored_service = EventBridgeService::new(
            &persistent_factory(&root),
            EventBridgeServiceDependencies {
                clock: Arc::new(FixedClock),
                dispatcher: restored_dispatcher.clone(),
                scheduler: restored_scheduler.clone(),
            },
        );
        restored_service.rule_store.load().expect("rule state should load");

        restored_service
            .restore_schedules()
            .expect("schedule restore should succeed");
        restored_service
            .restore_schedules()
            .expect("schedule restore should remain idempotent");

        assert_eq!(restored_scheduler.active_task_count(), 1);

        restored_scheduler.run_pending();

        assert_eq!(restored_dispatcher.delivery_count(), 1);
        assert_eq!(
            restored_dispatcher.run_payloads(),
            vec!["{\"kind\":\"scheduled\"}"]
        );
    }

    #[test]
    fn deleting_non_empty_rules_requires_target_removal_first() {
        let (service, _, _) = service("delete-order");
        let scope = scope();

        service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: Some(
                        "{\"source\":[\"orders\"]}".to_owned(),
                    ),
                    name: "live".to_owned(),
                    role_arn: None,
                    schedule_expression: None,
                    state: None,
                },
            )
            .expect("rule should create");
        service
            .put_targets(
                &scope,
                PutTargetsInput {
                    event_bus_name: None,
                    rule: "live".to_owned(),
                    targets: vec![lambda_target("processor")],
                },
            )
            .expect("target should store");
        assert!(service.delete_rule(&scope, "live", None).is_err());

        service
            .remove_targets(
                &scope,
                RemoveTargetsInput {
                    event_bus_name: None,
                    force: None,
                    ids: vec!["processor".to_owned()],
                    rule: "live".to_owned(),
                },
            )
            .expect("targets should remove");
        service.delete_rule(&scope, "live", None).expect("rule should delete");
    }

    #[test]
    fn target_payload_transformers_shape_delivery_bytes() {
        let (service, dispatcher, _) = service("transformer");
        let scope = scope();

        service
            .put_rule(
                &scope,
                PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: Some(
                        "{\"source\":[\"orders\"],\"detail\":{\"id\":[\"ord-1\"]}}"
                            .to_owned(),
                    ),
                    name: "transform".to_owned(),
                    role_arn: None,
                    schedule_expression: None,
                    state: None,
                },
            )
            .expect("rule should create");
        service
            .put_targets(
                &scope,
                PutTargetsInput {
                    event_bus_name: None,
                    rule: "transform".to_owned(),
                    targets: vec![EventBridgeTarget {
                        input_transformer: Some(
                            crate::EventBridgeInputTransformer {
                                input_paths_map: BTreeMap::from([
                                    (
                                        "id".to_owned(),
                                        "$.detail.id".to_owned(),
                                    ),
                                    (
                                        "source".to_owned(),
                                        "$.source".to_owned(),
                                    ),
                                ]),
                                input_template:
                                    "{\"source\":<source>,\"id\":<id>}"
                                        .to_owned(),
                            },
                        ),
                        ..lambda_target("processor")
                    }],
                },
            )
            .expect("target should store");

        service
            .put_events(
                &scope,
                PutEventsInput {
                    entries: vec![PutEventsRequestEntry {
                        detail: json!({ "id": "ord-1" }).to_string(),
                        detail_type: "Created".to_owned(),
                        event_bus_name: None,
                        resources: Vec::new(),
                        source: "orders".to_owned(),
                        time: None,
                        trace_header: None,
                    }],
                },
            )
            .expect("event should publish");

        assert_eq!(
            dispatcher.run_payloads(),
            vec!["{\"source\":\"orders\",\"id\":\"ord-1\"}"]
        );
    }
}
