# KodeBricks

## Setup Guide

- Copy [script file](script_file/install_kodebricks.sh) in a local folder.
- Open the terminal and move to that folder where `.sh` file is copied.
- run the command `chmod +x install_kodebricks.sh`.
- Then run the script file by running command `./install_kodebricks.sh`

Now Extension will be downloaded and a `main.py`, `mcp_config.json` and `KodeBricks-3.18.4.vsix` file will be created.
Incase the extension is not visible, right click on the `KodeBricks-3.18.4.vsix` in vs code and choose option `install extension vsix`

## KodeBricks setup
- Open the extension and add suitable LLM API key

## Setup MCP on KodeBricks

- Click on **MCP server** inside extension  
![Screenshot](assets/reference_images/1.png)

- Click on **Edit Global MCP**  
![Screenshot](assets/reference_images/2.png)

- A `mcp_settings.json` file will open, in that paste content of `mcp_config.json` created.  
![Screenshot](assets/reference_images/3.png)

- Inside `mcp_settings.json`:
  - In `env`, write your Databricks credentials

- Save this JSON file

- MCP is connected  
![Screenshot](assets/reference_images/4.png)
