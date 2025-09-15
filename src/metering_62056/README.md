# IEC 62056-21 Metering Module

This module implements support for the IEC 62056-21 protocol (formerly IEC 61107) for optical interface communication with electricity meters, specifically EasyMeter and EBZ devices.

## Overview

IEC 62056-21 is an international standard for exchanging data with utility meters using ASCII protocol over optical interface. The protocol supports multiple communication modes (A, B, C, D) with different baud rates and data exchange patterns.

## Supported Devices

### EasyMeter
- **Models**: Q3D, Q3B series
- **Communication**: 9600 baud, 7E1 (OBIS ASCI) or 8N1 (SML binary)
- **Supported Modes**: Mode C (bidirectional), Mode D (unidirectional push)
- **OBIS Codes**: Standard energy, power, voltage, and current measurements

### EBZ (eBZ GmbH)
- **Models**: DD3 series three-phase meters
- **Communication**: 9600 baud, 7E1
- **Supported Modes**: Mode C (bidirectional), Mode D (unidirectional push)
- **OBIS Codes**: Comprehensive three-phase measurements including power factor and reactive power

## Protocol Implementation

### Communication Modes

- **Mode A**: Fixed 300 baud, bidirectional ASCII
- **Mode B**: Extended mode A functionality
- **Mode C**: Variable baud rate after handshake, bidirectional
- **Mode D**: Fixed rate unidirectional push (2400/9600 baud)

### Message Format

```
/ESY5Q3D\@V5.3          # Identification line
0-0:1.0.0(210101120000W) # Timestamp
1-0:1.8.0(000123.456*kWh) # Energy consumed
1-0:15.7.0(001.234*kW)   # Current power
!                        # End of telegram
```

### OBIS Code Support

The module includes comprehensive OBIS code parsing and mapping for:
- Energy values (1-0:1.8.x, 1-0:2.8.x)
- Power measurements (1-0:x.7.0)
- Voltage and current per phase (1-0:3x.7.0, 1-0:x1.7.0)
- Device identification and timestamps (0-0:x.x.x)

## Integration

The module integrates with the energy2mqtt system by:

1. **MQTT Subscription**: Listens to `iec62056_input` topic for incoming telegrams
2. **Protocol Detection**: Automatically identifies meter type from manufacturer code
3. **Data Parsing**: Converts OBIS codes to structured metering data
4. **MQTT Publishing**: Forwards parsed data through the central MQTT manager

## Usage

The IEC62056Manager runs as a background task and processes incoming telegrams:

```rust
let mut iec62056 = Iec62056Manager::new(mqtt_sender);
iec62056.start_thread().await;
```

## File Structure

- `mod.rs` - Main manager and telegram parsing logic
- `structs.rs` - Data structures for device identification and OBIS data
- `utils.rs` - Utility functions for parsing and validation
- `obis_parser.rs` - OBIS code parsing and standard mappings
- `meter_definitions.rs` - Device-specific configurations for EasyMeter and EBZ

## Testing

The module includes comprehensive tests for:
- Telegram parsing with example data
- OBIS code validation and parsing
- Device identification and manufacturer detection
- Meter-specific configurations and mappings

Run tests with:
```bash
cargo test --package energy2mqtt --lib metering_62056
```