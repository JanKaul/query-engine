use arrow2::datatypes::DataType;
pub type Schema = Vec<Field>;

pub struct Field {
    pub data_type: DataType,
    pub name: String,
}
