mod common;

pub mod spot_rest;
pub mod spot_ws;
pub mod perps_rest;
pub mod perps_ws;

// Re-export main types for convenience
pub use spot_rest::KrakenSpotRest;
pub use spot_ws::KrakenSpotWs;
pub use perps_rest::KrakenPerpsRest;
pub use perps_ws::KrakenPerpsWs;