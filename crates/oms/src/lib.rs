use adapters::traits::{SpotRest, PerpRest};
use anyhow::Result;
use std::sync::Arc;

/// Order Management System that coordinates order operations across multiple adapters
pub struct OrderManager {
    spot_adapters: Vec<Arc<dyn SpotRest>>,
    perp_adapters: Vec<Arc<dyn PerpRest>>,
}

impl OrderManager {
    /// Create a new order manager
    pub fn new() -> Self {
        Self {
            spot_adapters: Vec::new(),
            perp_adapters: Vec::new(),
        }
    }
    
    /// Add a spot adapter
    pub fn add_spot_adapter(&mut self, adapter: Arc<dyn SpotRest>) {
        self.spot_adapters.push(adapter);
    }
    
    /// Add a perp adapter
    pub fn add_perp_adapter(&mut self, adapter: Arc<dyn PerpRest>) {
        self.perp_adapters.push(adapter);
    }
    
    /// Cancel all orders across all spot adapters
    pub async fn cancel_all_spot_orders(&self) -> Result<()> {
        log::info!("Cancelling all spot orders across {} adapters", self.spot_adapters.len());
        
        for (idx, adapter) in self.spot_adapters.iter().enumerate() {
            match adapter.cancel_all(None).await {
                Ok(count) => log::info!("Successfully cancelled {} spot orders on adapter {}", count, idx),
                Err(e) => log::error!("Failed to cancel spot orders on adapter {}: {}", idx, e),
            }
        }
        
        Ok(())
    }
    
    /// Cancel all orders across all perp adapters
    pub async fn cancel_all_perp_orders(&self) -> Result<()> {
        log::info!("Cancelling all perp orders across {} adapters", self.perp_adapters.len());
        
        for (idx, adapter) in self.perp_adapters.iter().enumerate() {
            match adapter.cancel_all(None).await {
                Ok(count) => log::info!("Successfully cancelled {} perp orders on adapter {}", count, idx),
                Err(e) => log::error!("Failed to cancel perp orders on adapter {}: {}", idx, e),
            }
        }
        
        Ok(())
    }
    
    /// Cancel all orders across all adapters (both spot and perp)
    pub async fn cancel_all_orders(&self) -> Result<()> {
        log::warn!("🛑 EMERGENCY: Cancelling all orders across all adapters");
        
        // Cancel spot orders
        self.cancel_all_spot_orders().await?;
        
        // Cancel perp orders
        self.cancel_all_perp_orders().await?;
        
        log::info!("All orders cancelled");
        Ok(())
    }
    
    /// Get number of registered adapters
    pub fn num_adapters(&self) -> (usize, usize) {
        (self.spot_adapters.len(), self.perp_adapters.len())
    }
}

impl Default for OrderManager {
    fn default() -> Self {
        Self::new()
    }
}

