use arrow2::datatypes::DataType;
pub type Schema = Vec<Field>;

#[derive(Clone)]
pub struct Field {
    pub data_type: DataType,
    pub name: String,
}
