#!/usr/bin/env bash
# Starts the API that Power BI reads. Leave this terminal open while you use Power BI.
set -euo pipefail
cd "$(dirname "$0")"
source venv/bin/activate
if [[ ! -d gold_export ]]; then echo "No results yet. Run first:  bash run_pipeline.sh"; exit 1; fi
if [[ ! -s .api_key ]]; then
  python -c "import secrets; print(secrets.token_hex(16))" > .api_key
  echo "A new API key was created in the file .api_key"
fi
export API_KEY="$(cat .api_key)"
echo "API key is in the file .api_key  (send it as the X-API-Key header)"
echo "Website      : http://localhost:8000/"
echo "Health check : http://localhost:8000/health"
echo "API test page: http://localhost:8000/docs"
exec uvicorn api.main:app --host 0.0.0.0 --port 8000
