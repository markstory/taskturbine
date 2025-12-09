use clap::Args;

use crate::CliError;
use taskturbine_core::api::Storage;

#[derive(Args, Debug)]
pub struct EmitEventArgs {
    /// The event name
    pub event_name: String,

    /// The event payload
    pub payload: String,
}

/// Emit an event to the storage system.
pub async fn emit_event(storage: Storage, args: EmitEventArgs) -> Result<(), CliError> {
    let res = storage.emit_event(&args.event_name, args.payload.as_bytes()).await;

    match res {
        Ok(_) => {
            println!("Emit event for {}", args.event_name);
            Ok(())
        },
        Err(err) => Err(CliError::Message(format!("Failed to emit event {err:?}"))),
    }
}
