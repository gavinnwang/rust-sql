use crate::page::table_page::TupleMetadata;

pub struct Tuple {
    data: Vec<u8>,
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

pub struct TupleRef<'a> {
    metadata: &'a TupleMetadata,
    data: &'a [u8],
}

impl<'a> TupleRef<'a> {
    pub(crate) fn new(data: &'a [u8], metadata: &'a TupleMetadata) -> TupleRef<'a> {
        TupleRef { data, metadata }
    }
    pub(crate) fn data(&self) -> &[u8] {
        self.data
    }

    pub(crate) fn metadata(&self) -> &TupleMetadata {
        self.metadata
    }
}
