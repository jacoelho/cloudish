#[derive(Debug, Default, Clone)]
pub(crate) struct XmlBuilder {
    buffer: String,
}

impl XmlBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn start(mut self, element: &str, xmlns: Option<&str>) -> Self {
        self.buffer.push('<');
        self.buffer.push_str(element);
        if let Some(xmlns) = xmlns {
            self.buffer.push_str(" xmlns=\"");
            self.buffer.push_str(&escape(xmlns));
            self.buffer.push('"');
        }
        self.buffer.push('>');
        self
    }

    pub(crate) fn end(mut self, element: &str) -> Self {
        self.buffer.push_str("</");
        self.buffer.push_str(element);
        self.buffer.push('>');
        self
    }

    pub(crate) fn elem(mut self, name: &str, value: &str) -> Self {
        self.buffer.push('<');
        self.buffer.push_str(name);
        self.buffer.push('>');
        self.buffer.push_str(&escape(value));
        self.buffer.push_str("</");
        self.buffer.push_str(name);
        self.buffer.push('>');
        self
    }

    pub(crate) fn raw(mut self, value: &str) -> Self {
        self.buffer.push_str(value);
        self
    }

    pub(crate) fn build(self) -> String {
        self.buffer
    }
}

pub(crate) fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());

    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(character),
        }
    }

    escaped
}

#[cfg(test)]
mod tests {
    use super::{XmlBuilder, escape};

    #[test]
    fn escape_special_xml_characters() {
        assert_eq!(
            escape("<tag foo='bar'>&\"baz\""),
            "&lt;tag foo=&apos;bar&apos;&gt;&amp;&quot;baz&quot;"
        );
    }

    #[test]
    fn build_xml_fragments() {
        let xml = XmlBuilder::new()
            .start("ErrorResponse", None)
            .start("Error", None)
            .elem("Code", "InvalidAction")
            .elem("Message", "bad <value>")
            .end("Error")
            .elem("RequestId", "abc")
            .end("ErrorResponse")
            .build();

        assert_eq!(
            xml,
            "<ErrorResponse><Error><Code>InvalidAction</Code><Message>bad &lt;value&gt;</Message></Error><RequestId>abc</RequestId></ErrorResponse>"
        );
    }

    #[test]
    fn append_raw_xml_fragments() {
        let xml = XmlBuilder::new()
            .start("Root", None)
            .raw("<Nested>ok</Nested>")
            .end("Root")
            .build();

        assert_eq!(xml, "<Root><Nested>ok</Nested></Root>");
    }
}
