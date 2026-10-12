# Build tests to make sure we got all needed data
FEATURES=$(sed -n "/^\[features\]/,/^\[/p" "Cargo.toml" | sed '$d' | grep '=[[:space:]]\[' | awk -F ' ' '{print $1}' | grep -v '^#' | grep -v default)

mkdir -p logs/

for feat in ${FEATURES}; do
    echo "Building feature ${feat}"
    cargo clean > /dev/null 2>&1
    cargo build --release --no-default-features -F ${feat} > logs/${feat} 2>&1

    if [ $? -ne 0 ]; then
        echo "FAILED TO BUILD feature ${feat}, see log logs/${feat}"
        exit 1
    fi
done
