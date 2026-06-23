import crypto_flow_bot.signal_visibility as _signal_visibility

_signal_visibility.install_trade_signal_visibility_patch()

from crypto_flow_bot.main import main

if __name__ == "__main__":
    main()
