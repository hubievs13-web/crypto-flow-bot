from crypto_flow_bot.main import main

from crypto_flow_bot.signal_visibility import install_trade_signal_visibility_patch

install_trade_signal_visibility_patch()

if __name__ == "__main__":
    main()
