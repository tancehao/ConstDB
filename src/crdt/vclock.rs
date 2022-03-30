use crate::Bytes;

type VClock<T> = MiniMap<T>;

#[allow(unused)]
pub struct MiniMap<T> {
    values: Vec<(u64, T)>,
}

impl<T: Default> Default for MiniMap<T> {
    fn default() -> Self {
        MiniMap {
            values: Vec::default(),
        }
    }
}

impl<T> MiniMap<T> {
    #[allow(unused)]
    pub fn get(&self, k: &u64) -> Option<&T> {
        match self.values.binary_search_by(|(key, _)| key.cmp(k)) {
            Ok(i) => self.values.get(i).map(|(_, x)| x),
            Err(_) => None,
        }
    }

    #[allow(unused)]
    pub fn get_mut(&mut self, k: &u64) -> Option<&mut T> {
        match self.values.binary_search_by(|(key, _)| key.cmp(k)) {
            Ok(i) => self.values.get_mut(i).map(|(_, x)| x),
            Err(_) => None,
        }
    }

    #[allow(unused)]
    pub fn set(&mut self, k: u64, v: T) {
        match self.values.binary_search_by(|(key, _)| key.cmp(&k)) {
            Ok(i) => self.values[i] = (k, v),
            Err(i) => self.values.insert(i, (k, v)),
        }
    }
}

#[allow(unused)]
type MultiValue = VClock<Bytes>;

impl MultiValue {
    #[allow(unused)]
    pub fn get_value(&self) -> Vec<Bytes> {
        self.values.iter().map(|(_, x)| x.clone()).collect()
    }
}
