# Download Syslogs

## Overview
The **Download Syslogs** repository provides a Python script to download system logs for specified devices. The downloaded logs are stored in a structured folder format for easy access.

---

## Repository Structure
- **`download_syslogs.py`**: The Python script to download system logs.
- **`SYS_LOGS/`**: Folder where downloaded system logs are saved (it is created automatically).

---

## Prerequisites
Ensure you have the following installed:
- Python 3.6 or higher
- The `DB` library from the `OAC` module:
  - You can copy the library from the experimental machine `ubuntu@10.100.11.212`.

---

## Setup
1. Clone or download the `Download_Syslogs` repository to your desired location:
   ```bash
   git clone <repository_url>
   ```

2. Navigate to the `Download_Syslogs` directory:
   ```bash
   cd Download_Syslogs
   ```

3. Set up the `OAC` module:
   - Copy the `DB` library from the experimental machine:
     ```bash
     scp ubuntu@10.100.11.212:/path/to/OAC/DB /your/local/path
     ```

---

## Usage

### Download Latest Syslogs
To download the latest syslogs for a specific device, use the following command:
```bash
python3 download_syslogs.py -d <device_id>
```
**Example:**
```bash
python3 download_syslogs.py -d 3633029274
```

### Download Syslogs from a Specific Start Date
To download syslogs starting from a specific date, use the following command:
```bash
python3 download_syslogs.py -d <device_id> -sd "<start_date>"
```
**Example:**
```bash
python3 download_syslogs.py -d 3633029274 -sd "2024-11-12"
```

---

## Output
After successfully running the script, the syslogs will be saved at:
```
./Download_Syslogs/SYS_LOGS/<device_id>/<syslogs_zip_file>
```
You can access and download the syslogs from this location.

---

## Notes
- Replace `<device_id>` with the actual ID of the device.
- Use the appropriate date format for the `-sd` option (YYYY-MM-DD).

---
