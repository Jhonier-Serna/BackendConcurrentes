from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db.mongodb import connect_to_mongo
from app.routes import user, gene_search, file_upload

connect_to_mongo()
app = FastAPI(
    title="Gene Search Backend for Vineyard Research",
    description="Backend for searching and analyzing gene data from grape varieties",
    version="0.1.0"
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(user.router, prefix="/users", tags=["users"])
app.include_router(gene_search.router, prefix="/search", tags=["gene-search"])
app.include_router(file_upload.router, prefix="/upload", tags=["file-upload"])

@app.get("/")
async def root():
    return {
        "message": "Welcome to Gene Search Backend",
        "status": "operational"
    }
