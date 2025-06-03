#!/bin/bash

# ---------- CONFIG ----------
REPO_MAIN_URL="https://raw.githubusercontent.com/HemantGuptaShorthillsAI/Kode-Bricks/main/MCP-Servers/main.py"
REPO_REQ_URL="https://raw.githubusercontent.com/HemantGuptaShorthillsAI/Kode-Bricks/main/MCP-Servers/requirements.txt"
VSIX_URL="https://github.com/HemantGuptaShorthillsAI/KodeBricks/raw/main/Extension-KodeBricks/KodeBricks-3.18.4.vsix"
MAIN_FILE="main.py"
REQUIREMENTS_FILE="requirements.txt"
VSIX_FILE="KodeBricks-3.18.4.vsix"
CONFIG_FILE="mcp_config.json" 
# ----------------------------

echo "🔧 Checking for 'uv'..."
if ! command -v uv &> /dev/null; then
    echo "📦 'uv' not found. Installing using pip..."
    pip install uv || { echo "❌ pip not found or failed to install uv"; exit 1; }
else
    echo "✅ 'uv' is already installed."
fi

echo "⬇️ Downloading requirements.txt ..."
curl -L "$REPO_REQ_URL" -o "$REQUIREMENTS_FILE"
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "📦 Installing packages from $REQUIREMENTS_FILE..."
    pip3 install -r "$REQUIREMENTS_FILE"
    echo "✅ Packages installed."
else
    echo "❌ Failed to download $REQUIREMENTS_FILE"
    exit 1
fi

echo "⬇️ Downloading main.py from GitHub..."
curl -L "$REPO_MAIN_URL" -o "$MAIN_FILE"
if [ $? -ne 0 ] || ! grep -q "def " "$MAIN_FILE"; then
    echo "❌ main.py is not valid. Check the URL or content."
    exit 1
fi
MAIN_ABS_PATH=$(realpath "$MAIN_FILE")
echo "✅ main.py downloaded to $MAIN_ABS_PATH"

echo "⬇️ Downloading KodeBricks VSIX extension..."
curl -L "$VSIX_URL" -o "$VSIX_FILE"
if [ $? -ne 0 ]; then
    echo "❌ Failed to download VSIX extension. Check the VSIX_URL."
    exit 1
fi
echo "✅ Extension downloaded as $VSIX_FILE"

echo "💻 Installing KodeBricks extension in VS Code..."
code --install-extension "$VSIX_FILE" --force
if [ $? -eq 0 ]; then
    echo "✅ KodeBricks extension installed."
else
    echo "❌ VS Code CLI not found or install failed. Make sure 'code' is in your PATH."
fi

echo "🔍 Finding path of 'uv'..."
UV_PATH=$(which uv)
echo "✅ UV path: $UV_PATH"

echo "📝 Creating $CONFIG_FILE..."
cat > $CONFIG_FILE <<EOF
{
  "mcpServers": {
    "Databricks-server": {
      "command": "$UV_PATH",
      "args": [
        "run",
        "--with",
        "mcp[cli]",
        "--with",
        "requests",
        "mcp",
        "run",
        "$MAIN_ABS_PATH"
      ],
      "env": {
        "DATABRICKS_INSTANCE": "DATABRICKS_INSTANCE",
        "DATABRICKS_TOKEN": "DATABRICKS_TOKEN"
      }
    }
  }
}
EOF

echo "✅ $CONFIG_FILE has been created successfully."
