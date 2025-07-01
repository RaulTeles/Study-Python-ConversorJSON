from fastapi import APIRouter
from .service.sumarizacao_case1 import router as sumarizacao_router
from .service.sumarizacao_case1_workflow import router as sumarizacao_router1
from .service.sumarizacao_case2 import router as sumarizacao_router2
from .service.sumarizacao_case2_workflow import router as sumarizacao_router3
#from .service.sumarizacao_case2_workflow import router as sumarizacao_router3

router = APIRouter()

router.include_router(sumarizacao_router, prefix="/api")
router.include_router(sumarizacao_router1, prefix="/api")
router.include_router(sumarizacao_router2, prefix="/api")
router.include_router(sumarizacao_router3, prefix="/api")