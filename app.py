from flask import Flask
import threading
import crypto_research_bot_final  # import bot code

app = Flask(__name__)

@app.route('/')
def index():
    return "Crypto Research Bot is running!"

if __name__ == '__main__':
    # chạy bot song song với Flask
    bot_thread = threading.Thread(target=crypto_research_bot_final.main)  # nhớ trong bot code nên có hàm main()
    bot_thread.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
