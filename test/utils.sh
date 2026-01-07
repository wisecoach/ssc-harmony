function show_tx_error() {
    find -name "ssc*.log" | xargs grep -a "$1" | grep "error"
}