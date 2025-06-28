#!/bin/bash
# パイプラインファイルのパス初期化スクリプト
# Dockerコンテナ内でパイプラインファイルが確実に見つかるように
# 複数のパスにシンボリックリンクを作成する

set -e

echo "[INIT] Pipeline path initialization starting..."

# 元のパイプラインファイルの場所を特定
PIPELINE_SOURCES=(
    "/tests/src/dev/pipeline"
    "/app/src/dev/pipeline" 
    "/src/dev/pipeline"
)

PIPELINE_SOURCE=""
for source in "${PIPELINE_SOURCES[@]}"; do
    echo "[INIT] Checking $source..."
    if [ -d "$source" ]; then
        echo "[INIT]   Directory exists: $source"
        file_count=$(find "$source" -name "*.json" 2>/dev/null | wc -l)
        echo "[INIT]   JSON files found: $file_count"
        if [ "$file_count" -gt 0 ]; then
            echo "[INIT] Found pipeline source directory with files: $source"
            PIPELINE_SOURCE="$source"
            break
        else
            echo "[INIT]   Directory is empty or no JSON files found"
        fi
    else
        echo "[INIT]   Directory does not exist: $source"
    fi
done

if [ -z "$PIPELINE_SOURCE" ]; then
    echo "[ERROR] No pipeline source directory with JSON files found. Checked:"
    for source in "${PIPELINE_SOURCES[@]}"; do
        echo "  - $source (exists: $([ -d "$source" ] && echo "yes" || echo "no"))"
        if [ -d "$source" ]; then
            echo "    Files: $(ls -la "$source" 2>/dev/null | tail -n +2 | wc -l) items"
        fi
    done
    
    # 追加の検索場所を試す
    echo "[INIT] Searching for JSON files in alternative locations..."
    additional_paths=(
        "/app"
        "/tests"
        "/app/src"
        "/tests/src"
    )
    
    for path in "${additional_paths[@]}"; do
        if [ -d "$path" ]; then
            echo "[INIT] Searching in $path..."
            json_files=$(find "$path" -name "*.json" -path "*/pipeline/*" 2>/dev/null)
            if [ -n "$json_files" ]; then
                echo "[INIT] Found JSON files in $path:"
                echo "$json_files" | head -3
                # 最初に見つかったディレクトリを使用
                PIPELINE_SOURCE=$(dirname $(echo "$json_files" | head -1))
                echo "[INIT] Using alternative source: $PIPELINE_SOURCE"
                break
            fi
        fi
    done
    
    # 最後の手段：ワイルドカード検索
    if [ -z "$PIPELINE_SOURCE" ]; then
        echo "[INIT] Last resort: wildcard search for pipeline JSON files..."
        potential_files=$(find /app /tests -name "pi_*.json" -o -name "*pipeline*.json" 2>/dev/null | head -5)
        if [ -n "$potential_files" ]; then
            echo "[INIT] Found potential pipeline files:"
            echo "$potential_files"
            # 最初に見つかったファイルのディレクトリを使用
            PIPELINE_SOURCE=$(dirname $(echo "$potential_files" | head -1))
            echo "[INIT] Using last resort source: $PIPELINE_SOURCE"
        fi
    fi
fi

if [ -z "$PIPELINE_SOURCE" ]; then
    echo "[ERROR] Still no pipeline files found. Exiting."
    exit 1
fi

# 目標パスリスト（テストで探索されるパス）
TARGET_PATHS=(
    "/tests/src/dev/pipeline"
    "/app/src/dev/pipeline"
)

# シンボリックリンクを作成
for target in "${TARGET_PATHS[@]}"; do
    if [ "$target" != "$PIPELINE_SOURCE" ]; then
        # ディレクトリ構造を作成
        mkdir -p "$(dirname "$target")"
        
        # 既存のファイル/ディレクトリを削除
        if [ -e "$target" ]; then
            rm -rf "$target"
        fi
        
        # シンボリックリンクを作成
        ln -sf "$PIPELINE_SOURCE" "$target"
        echo "[INIT] Created symlink: $target -> $PIPELINE_SOURCE"
    fi
done

# 検証
echo "[INIT] Verification:"
for target in "${TARGET_PATHS[@]}"; do
    if [ -d "$target" ]; then
        file_count=$(ls -1 "$target"/*.json 2>/dev/null | wc -l)
        echo "[INIT] $target: $file_count JSON files found"
    else
        echo "[INIT] $target: NOT FOUND"
    fi
done

echo "[INIT] Pipeline path initialization completed successfully!"
