use log::error;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub enum DefaultFunction {
    Set,
    Sleep,
}

#[derive(Deserialize, Clone)]
pub struct DefaultElement {
    pub function: DefaultFunction,
    pub register: Option<String>,
    pub value: String,
}

#[derive(Deserialize, Clone)]
pub struct Defaults {
    pub defaults: Vec<DefaultElement>
}

impl Defaults {
    pub fn new(file: &String) -> Self {
        let config = match std::fs::read_to_string(file) {
            Ok(content) => {
                match serde_yml::from_str::<Defaults>(&content) {
                    Ok(defaults) => defaults,
                    Err(e) => {
                        error!("The defaults file {file} is not parseable");
                        error!("{e:?}");
                        return Defaults::default();
                    },
                }
            },
            Err(e) => {
                error!("Default file {file} could not be parsed");
                return Defaults::default();
            }
        };

        config
    }

}

impl Default for Defaults {
    fn default() -> Self {
        Self { defaults: Vec::new() }
    }
}