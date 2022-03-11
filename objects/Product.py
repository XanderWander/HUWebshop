from dataclasses import dataclass
import datetime


@dataclass
class Product:
    id: int
    _id: str
    deeplink: str
    description: str
    fast_mover: bool
    herhaalaankopen: bool
    name: str
    predict_oos_date: datetime
    price_discount: int
    price_mrsp: int
    selling_price: int


