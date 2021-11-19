use super::string::StringArray;
use hashbrown::HashMap;

#[derive(Debug, Default)]
pub struct StringDictionary {
    dedup: HashMap<String, usize>,
    data: StringArray,
}

impl StringDictionary {
    pub fn new() -> Self {
        return Default::default();
    }

    pub fn lookup_or_insert(&mut self, value: &str) -> usize {
        let found = self.lookup(value);
        return match found {
            None => {
                let id = self.data.append(value) + 1;
                self.dedup.insert(value.to_string(), id);
                id
            }
            Some(id) => id,
        };
    }

    pub fn lookup(&self, value: &str) -> Option<usize> {
        return self.dedup.get(value).map(|x| x.to_owned());
    }

    pub fn get(&self, id: usize) -> Option<&str> {
        return self.data.get(id - 1);
    }
}

#[cfg(test)]
mod tests {
    use super::StringDictionary;

    #[test]
    fn test_storage() {
        let mut dict = StringDictionary::new();
        let id = dict.lookup_or_insert("hello, world");
        let id2 = dict.lookup_or_insert("hello, world");
        assert_eq!(id, id2);
        let id3 = dict.lookup_or_insert("hello world");
        assert_ne!(id, id3);
        let v1 = dict.get(id);
        let v2 = dict.get(id2);
        assert_eq!(v1, v2);
    }
}
