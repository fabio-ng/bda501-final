#!/bin/bash

echo "=== Phase 5 FastAPI Backend Verification ==="
echo ""

# Check directory structure
echo "1. Checking directory structure..."
dirs=(
    "api"
    "api/routers"
    "api/services"
    "api/middleware"
)

for dir in "${dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo "   ✓ $dir/"
    else
        echo "   ✗ $dir/ NOT FOUND"
    fi
done
echo ""

# Check core files
echo "2. Checking core application files..."
core_files=(
    "api/__init__.py"
    "api/main.py"
    "api/config.py"
    "api/database.py"
    "api/models.py"
    "api/Dockerfile"
    "api/requirements.txt"
    "api/.env.example"
)

for file in "${core_files[@]}"; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        echo "   ✓ $file ($lines lines)"
    else
        echo "   ✗ $file NOT FOUND"
    fi
done
echo ""

# Check routers
echo "3. Checking routers..."
routers=(
    "api/routers/__init__.py"
    "api/routers/health.py"
    "api/routers/predict.py"
    "api/routers/model.py"
    "api/routers/predictions.py"
    "api/routers/alerts.py"
    "api/routers/ingest.py"
    "api/routers/dashboard.py"
)

for file in "${routers[@]}"; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        echo "   ✓ $file ($lines lines)"
    else
        echo "   ✗ $file NOT FOUND"
    fi
done
echo ""

# Check services
echo "4. Checking services..."
services=(
    "api/services/__init__.py"
    "api/services/predictor.py"
    "api/services/model_loader.py"
    "api/services/prediction_logger.py"
)

for file in "${services[@]}"; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        echo "   ✓ $file ($lines lines)"
    else
        echo "   ✗ $file NOT FOUND"
    fi
done
echo ""

# Check middleware
echo "5. Checking middleware..."
middleware=(
    "api/middleware/__init__.py"
    "api/middleware/correlation.py"
    "api/middleware/logging.py"
)

for file in "${middleware[@]}"; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        echo "   ✓ $file ($lines lines)"
    else
        echo "   ✗ $file NOT FOUND"
    fi
done
echo ""

# Check deployment files
echo "6. Checking deployment files..."
deploy_files=(
    "docker-compose.yml"
    "init_db.sql"
    "API_README.md"
    "PHASE5_SUMMARY.md"
)

for file in "${deploy_files[@]}"; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        echo "   ✓ $file ($lines lines)"
    else
        echo "   ✗ $file NOT FOUND"
    fi
done
echo ""

# Check syntax of Python files
echo "7. Checking Python syntax..."
python_files=$(find api -name "*.py" -type f)
syntax_errors=0

for file in $python_files; do
    if python -m py_compile "$file" 2>/dev/null; then
        echo "   ✓ $file"
    else
        echo "   ✗ $file - SYNTAX ERROR"
        syntax_errors=$((syntax_errors + 1))
    fi
done
echo ""

if [ $syntax_errors -eq 0 ]; then
    echo "✓ All Python files have valid syntax"
else
    echo "✗ $syntax_errors files have syntax errors"
fi
echo ""

# Summary
echo "=== Verification Complete ==="
total_python=$(find api -name "*.py" -type f | wc -l)
total_lines=$(find api -name "*.py" -type f -exec wc -l {} + | tail -1 | awk '{print $1}')
echo "Total Python files: $total_python"
echo "Total lines of code: $total_lines"
echo ""
echo "Phase 5 Backend is ready for deployment!"
