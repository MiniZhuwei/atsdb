use std::str::from_utf8;
use std::usize;

#[derive(Debug)]
pub struct StringArray {
    offsets: Vec<usize>,
    data: Vec<u8>,
}

impl Default for StringArray {
    fn default() -> Self {
        return Self {
            offsets: vec![0],
            data: Vec::<u8>::new(),
        };
    }
}

impl StringArray {
    pub fn new() -> Self {
        return Self::default();
    }

    pub fn append(&mut self, value: &str) -> usize {
        let id = self.offsets.len() - 1;
        let end = self.offsets[id] + value.len();
        self.offsets.push(end);
        self.data.append(value.as_bytes().to_owned().as_mut());
        return id;
    }

    pub fn get(&self, id: usize) -> Option<&str> {
        let offset = self.offsets.get(id)?;
        let end = self.offsets.get(id + 1)?;
        return Some(from_utf8(&self.data[*offset..*end]).unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::StringArray;

    #[test]
    fn test_storage() {
        let mut array = StringArray::new();
        let id1 = array.append("hello, world");
        let id2 = array.append("one");
        assert_eq!(array.get(id1).unwrap(), "hello, world");
        assert_eq!(array.get(id2).unwrap(), "one");
        let empty_array = StringArray::new();
        assert_eq!(empty_array.get(0), None);
    }
}
