from fastapi import FastAPI
from app.routes import router as main_router

app = FastAPI(  
    title="Vale COI API",   
    version="1.0.0"  
)  

app.include_router(main_router)