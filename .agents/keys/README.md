# Google Earth Engine Credentials for Antigravity MCP

To enable the `google-earth-engine` MCP server:

1. In the [Google Cloud Console](https://console.cloud.google.com/):
   - Select or create your project (e.g. `fcl-bloomtech-gis`).
   - Enable the **Google Earth Engine API**.
   - Create a Service Account under **IAM & Admin > Service Accounts**.
   - Assign the **Earth Engine Resource Viewer** (or **Earth Engine User**) role.
   - Go to the **Keys** tab and select **Add Key > Create new key > JSON**.

2. Save your downloaded JSON key file directly in this folder as:
   ```
   .agents/keys/earth-engine-key.json
   ```
   *(Note: This file is automatically ignored by `.gitignore` to prevent committing secrets).*

3. If your Google Cloud Project ID is different from `fcl-bloomtech-gis`, update the `GOOGLE_CLOUD_PROJECT` value in `.agents/mcp_config.json`.
