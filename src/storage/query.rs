#[derive(Debug)]
pub(crate) enum Matcher<'a> {
    LiteralEqual(Option<&'a str>),
    LiteralNotEqual(Option<&'a str>),
    RegexMatch(&'a str),
    RegexNotMatch(&'a str),
}
