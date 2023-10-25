#[derive(Clone)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

#[derive(Clone)]
pub enum Value {
    Integer(i32),
    Float(f32),
    Text(String),
    Boolean(bool),
}
