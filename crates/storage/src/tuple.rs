use crate::page::table_page::TupleMetadata;

pub struct Tuple {
    data: Vec<u8>,
}

pub struct TupleRef<'a> {
    pub metadata: &'a TupleMetadata,
    pub data: &'a [u8],
}

impl Tuple {
    pub(crate) fn new(data: Vec<u8>) -> Tuple {
        Tuple { data }
    }

    pub(crate) fn tuple_size(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn data(&self) -> &Vec<u8> {
        &self.data
    }

    pub(crate) fn data_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}
