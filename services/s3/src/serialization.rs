use csv::{ReaderBuilder, Terminator, WriterBuilder};
use regex_lite::Regex;

use crate::{
    errors::S3Error, object_read_model::ObjectReadRequest, scope::S3Scope,
    state::S3Service,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvFileHeaderInfo {
    Ignore,
    None,
    Use,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvInputSerialization {
    pub comments: Option<char>,
    pub field_delimiter: char,
    pub file_header_info: CsvFileHeaderInfo,
    pub quote_character: char,
    pub quote_escape_character: char,
    pub record_delimiter: char,
}

impl Default for CsvInputSerialization {
    fn default() -> Self {
        Self {
            comments: Some('#'),
            field_delimiter: ',',
            file_header_info: CsvFileHeaderInfo::None,
            quote_character: '"',
            quote_escape_character: '"',
            record_delimiter: '\n',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvOutputSerialization {
    pub always_quote: bool,
    pub field_delimiter: char,
    pub quote_character: char,
    pub quote_escape_character: char,
    pub record_delimiter: char,
}

impl Default for CsvOutputSerialization {
    fn default() -> Self {
        Self {
            always_quote: false,
            field_delimiter: ',',
            quote_character: '"',
            quote_escape_character: '"',
            record_delimiter: '\n',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectObjectContentInput {
    pub bucket: String,
    pub csv_input: CsvInputSerialization,
    pub csv_output: CsvOutputSerialization,
    pub expression: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectObjectStats {
    pub bytes_processed: u64,
    pub bytes_returned: u64,
    pub bytes_scanned: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectObjectContentOutput {
    pub records: String,
    pub stats: SelectObjectStats,
}

impl S3Service {
    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn select_object_content(
        &self,
        scope: &S3Scope,
        input: SelectObjectContentInput,
    ) -> Result<SelectObjectContentOutput, S3Error> {
        let object = self.get_object(
            scope,
            &ObjectReadRequest {
                bucket: input.bucket.clone(),
                key: input.key.clone(),
                version_id: input.version_id.clone(),
                ..ObjectReadRequest::default()
            },
        )?;
        evaluate_csv_select(&object.body, &input)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn restore_object(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), S3Error> {
        let _ = self.get_object(
            scope,
            &ObjectReadRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id: version_id.map(str::to_owned),
                ..ObjectReadRequest::default()
            },
        )?;
        Err(S3Error::InvalidArgument {
            code: "NotImplemented",
            message: "RestoreObject lifecycle is not implemented.".to_owned(),
            status_code: 501,
        })
    }
}

fn evaluate_csv_select(
    body: &[u8],
    input: &SelectObjectContentInput,
) -> Result<SelectObjectContentOutput, S3Error> {
    let query = parse_select_query(&input.expression)?;
    let mut reader = ReaderBuilder::new();
    let has_headers = matches!(
        input.csv_input.file_header_info,
        CsvFileHeaderInfo::Ignore | CsvFileHeaderInfo::Use
    );
    reader
        .has_headers(has_headers)
        .delimiter(csv_byte(
            input.csv_input.field_delimiter,
            "FieldDelimiter",
        )?)
        .quote(csv_byte(input.csv_input.quote_character, "QuoteCharacter")?)
        .escape(Some(csv_byte(
            input.csv_input.quote_escape_character,
            "QuoteEscapeCharacter",
        )?))
        .terminator(Terminator::Any(csv_byte(
            input.csv_input.record_delimiter,
            "RecordDelimiter",
        )?));
    if let Some(comment) = input.csv_input.comments {
        reader.comment(Some(csv_byte(comment, "Comments")?));
    }

    let mut reader = reader.from_reader(body);
    let all_headers = if has_headers {
        Some(
            reader
                .headers()
                .map_err(|error| {
                    invalid_select_input_error(error.to_string())
                })?
                .iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let visible_headers =
        matches!(input.csv_input.file_header_info, CsvFileHeaderInfo::Use)
            .then(|| all_headers.clone().unwrap_or_default());

    let mut writer = WriterBuilder::new();
    writer
        .delimiter(csv_byte(
            input.csv_output.field_delimiter,
            "FieldDelimiter",
        )?)
        .quote(csv_byte(input.csv_output.quote_character, "QuoteCharacter")?)
        .escape(csv_byte(
            input.csv_output.quote_escape_character,
            "QuoteEscapeCharacter",
        )?)
        .quote_style(if input.csv_output.always_quote {
            csv::QuoteStyle::Always
        } else {
            csv::QuoteStyle::Necessary
        })
        .terminator(Terminator::Any(csv_byte(
            input.csv_output.record_delimiter,
            "RecordDelimiter",
        )?));
    let mut writer = writer.from_writer(Vec::new());

    let mut matched = 0usize;
    for record in reader.records() {
        let record = record
            .map_err(|error| invalid_select_input_error(error.to_string()))?;
        if !query.matches(&record, visible_headers.as_deref())? {
            continue;
        }
        query.write_projection(
            &record,
            visible_headers.as_deref(),
            &mut writer,
        )?;
        matched += 1;
        if query.limit.is_some_and(|limit| matched >= limit) {
            break;
        }
    }

    let records = String::from_utf8(
        writer
            .into_inner()
            .map_err(|error| invalid_select_input_error(error.to_string()))?,
    )
    .map_err(|error| invalid_select_input_error(error.to_string()))?;

    Ok(SelectObjectContentOutput {
        stats: SelectObjectStats {
            bytes_processed: body.len() as u64,
            bytes_returned: records.len() as u64,
            bytes_scanned: body.len() as u64,
        },
        records,
    })
}

fn csv_byte(value: char, name: &str) -> Result<u8, S3Error> {
    if !value.is_ascii() {
        return Err(invalid_select_input_error(format!(
            "{name} must be a single-byte ASCII character."
        )));
    }
    Ok(value as u8)
}

#[derive(Debug, Clone, PartialEq)]
struct SelectQuery {
    condition: Option<SelectCondition>,
    limit: Option<usize>,
    projection: SelectProjection,
}

impl SelectQuery {
    fn matches(
        &self,
        record: &csv::StringRecord,
        headers: Option<&[String]>,
    ) -> Result<bool, S3Error> {
        let Some(condition) = &self.condition else {
            return Ok(true);
        };
        let lhs = select_record_value(record, headers, &condition.column)?;
        Ok(condition.matches(lhs))
    }

    fn write_projection(
        &self,
        record: &csv::StringRecord,
        headers: Option<&[String]>,
        writer: &mut csv::Writer<Vec<u8>>,
    ) -> Result<(), S3Error> {
        match &self.projection {
            SelectProjection::All => {
                writer.write_record(record.iter()).map_err(|error| {
                    invalid_select_input_error(error.to_string())
                })
            }
            SelectProjection::Columns(columns) => {
                let values = columns
                    .iter()
                    .map(|column| {
                        select_record_value(record, headers, column)
                            .map(str::to_owned)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                writer.write_record(values).map_err(|error| {
                    invalid_select_input_error(error.to_string())
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SelectProjection {
    All,
    Columns(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
struct SelectCondition {
    column: String,
    operator: SelectOperator,
    value: SelectValue,
}

impl SelectCondition {
    fn matches(&self, lhs: &str) -> bool {
        match (&self.operator, &self.value) {
            (SelectOperator::Eq, SelectValue::String(rhs)) => lhs == rhs,
            (SelectOperator::Eq, SelectValue::Number(rhs)) => lhs
                .parse::<f64>()
                .ok()
                .is_some_and(|lhs| (lhs - rhs).abs() < f64::EPSILON),
            (SelectOperator::Gt, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs > *rhs)
            }
            (SelectOperator::Ge, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs >= *rhs)
            }
            (SelectOperator::Lt, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs < *rhs)
            }
            (SelectOperator::Le, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs <= *rhs)
            }
            (SelectOperator::Gt, SelectValue::String(rhs)) => lhs > rhs,
            (SelectOperator::Ge, SelectValue::String(rhs)) => lhs >= rhs,
            (SelectOperator::Lt, SelectValue::String(rhs)) => lhs < rhs,
            (SelectOperator::Le, SelectValue::String(rhs)) => lhs <= rhs,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SelectOperator {
    Eq,
    Ge,
    Gt,
    Le,
    Lt,
}

#[derive(Debug, Clone, PartialEq)]
enum SelectValue {
    Number(f64),
    String(String),
}

fn parse_select_query(expression: &str) -> Result<SelectQuery, S3Error> {
    static SELECT_REGEX: std::sync::OnceLock<Result<Regex, String>> =
        std::sync::OnceLock::new();
    static WHERE_REGEX: std::sync::OnceLock<Result<Regex, String>> =
        std::sync::OnceLock::new();

    let select_regex = SELECT_REGEX
        .get_or_init(|| {
            Regex::new(
                "(?is)^select\\s+(.+?)\\s+from\\s+s3object(?:\\s+([A-Za-z_][A-Za-z0-9_]*))?(?:\\s+where\\s+(.+?))?(?:\\s+limit\\s+(\\d+))?\\s*$",
            )
            .map_err(|error| {
                format!("select regex failed to compile: {error}")
            })
        })
        .as_ref()
        .map_err(|message| S3Error::Internal {
            message: message.clone(),
        })?;
    let where_regex = WHERE_REGEX
        .get_or_init(|| {
            Regex::new("(?is)^(.+?)\\s*(<=|>=|=|<|>)\\s*(.+?)\\s*$").map_err(
                |error| format!("where regex failed to compile: {error}"),
            )
        })
        .as_ref()
        .map_err(|message| S3Error::Internal { message: message.clone() })?;

    let captures =
        select_regex.captures(expression.trim()).ok_or_else(|| {
            invalid_select_input_error(
                "Unsupported SELECT expression.".to_owned(),
            )
        })?;

    let alias = captures.get(2).map(|capture| capture.as_str().to_owned());
    let projection = captures
        .get(1)
        .ok_or_else(|| {
            invalid_select_input_error(
                "Unsupported SELECT projection.".to_owned(),
            )
        })?
        .as_str()
        .trim();
    let projection = if projection == "*" {
        SelectProjection::All
    } else {
        SelectProjection::Columns(
            projection
                .split(',')
                .map(|column| {
                    normalize_select_identifier(
                        column.trim(),
                        alias.as_deref(),
                    )
                })
                .collect(),
        )
    };
    let condition = captures
        .get(3)
        .map(|capture| {
            let where_captures = where_regex
                .captures(capture.as_str().trim())
                .ok_or_else(|| {
                    invalid_select_input_error(
                        "Unsupported WHERE clause.".to_owned(),
                    )
                })?;
            let column = normalize_select_identifier(
                where_captures
                    .get(1)
                    .ok_or_else(|| {
                        invalid_select_input_error(
                            "Unsupported WHERE left-hand side.".to_owned(),
                        )
                    })?
                    .as_str(),
                alias.as_deref(),
            );
            let operator = match where_captures
                .get(2)
                .ok_or_else(|| {
                    invalid_select_input_error(
                        "Unsupported WHERE operator.".to_owned(),
                    )
                })?
                .as_str()
            {
                "=" => SelectOperator::Eq,
                ">" => SelectOperator::Gt,
                ">=" => SelectOperator::Ge,
                "<" => SelectOperator::Lt,
                "<=" => SelectOperator::Le,
                _ => {
                    return Err(invalid_select_input_error(
                        "Unsupported WHERE operator.".to_owned(),
                    ));
                }
            };
            let raw_value = where_captures
                .get(3)
                .ok_or_else(|| {
                    invalid_select_input_error(
                        "Unsupported WHERE right-hand side.".to_owned(),
                    )
                })?
                .as_str()
                .trim();
            let value = if raw_value.starts_with('\'')
                && raw_value.ends_with('\'')
                && raw_value.len() >= 2
            {
                SelectValue::String(
                    raw_value[1..raw_value.len() - 1].replace("''", "'"),
                )
            } else {
                SelectValue::Number(raw_value.parse::<f64>().map_err(
                    |_| {
                        invalid_select_input_error(
                            "Unsupported WHERE value.".to_owned(),
                        )
                    },
                )?)
            };

            Ok(SelectCondition { column, operator, value })
        })
        .transpose()?;
    let limit = captures
        .get(4)
        .map(|capture| {
            capture.as_str().parse::<usize>().map_err(|_| {
                invalid_select_input_error(
                    "LIMIT must be a positive integer.".to_owned(),
                )
            })
        })
        .transpose()?;

    Ok(SelectQuery { condition, limit, projection })
}

fn normalize_select_identifier(value: &str, alias: Option<&str>) -> String {
    let value = value.trim();
    let value = alias
        .and_then(|alias| value.strip_prefix(&format!("{alias}.")))
        .unwrap_or(value);
    value.trim_matches('"').to_owned()
}

fn select_record_value<'a>(
    record: &'a csv::StringRecord,
    headers: Option<&[String]>,
    column: &str,
) -> Result<&'a str, S3Error> {
    if let Some(position) = column.strip_prefix('_') {
        let index = position.parse::<usize>().map_err(|_| {
            invalid_select_input_error(format!(
                "Unsupported column `{column}`."
            ))
        })?;
        return record.get(index.saturating_sub(1)).ok_or_else(|| {
            invalid_select_input_error(format!(
                "Column `{column}` is out of bounds."
            ))
        });
    }

    let headers = headers.ok_or_else(|| {
        invalid_select_input_error(format!(
            "Column `{column}` requires CSV headers."
        ))
    })?;
    let index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            invalid_select_input_error(format!(
                "Column `{column}` does not exist."
            ))
        })?;
    record.get(index).ok_or_else(|| {
        invalid_select_input_error(format!(
            "Column `{column}` does not exist."
        ))
    })
}

fn invalid_select_input_error(message: String) -> S3Error {
    S3Error::InvalidArgument {
        code: "InvalidArgument",
        message,
        status_code: 400,
    }
}
