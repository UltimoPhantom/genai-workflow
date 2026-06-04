from fastapi import FastAPI

app = FastAPI(title="GenAI Pipeline Gateway")

@app.get("/health")
def health():
    return {"status": "ok"}
