//! Models potentially stored in the archive.
use errors::BufkitDataErr;

use std::fmt;

/// Models potentially stored in the archive.
#[allow(missing_docs)]
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Model {
    GFS,
    NAM,
    NAM4KM,
}

impl fmt::Display for Model {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Model::*;

        match self {
            GFS => write!(f, "{}", stringify!(GFS)),
            NAM => write!(f, "{}", stringify!(NAM)),
            NAM4KM => write!(f, "{}", stringify!(NAM4KM)),
        }
    }
}

impl Model {
    /// Get the string representation of the model.
    pub fn string_name(self) -> &'static str {
        match self {
            Model::GFS => "gfs",
            Model::NAM => "nam",
            Model::NAM4KM => "nam4km",
        }
    }

    /// Convert from a string to an enum value.
    ///
    /// This function recognizes some old, currently unused model names.
    pub fn string_to_enum(test_str: &str) -> Result<Model, BufkitDataErr> {
        match test_str {
            "gfs" | "gfs3" | "GFS" | "GFS3" => Ok(Model::GFS),
            "nam" | "namm" | "NAM" | "NAMM" => Ok(Model::NAM),
            "nam4km" | "NAM4KM" => Ok(Model::NAM4KM),
            _ => Err(BufkitDataErr::InvalidModelName(test_str.to_owned())),
        }
    }
}
