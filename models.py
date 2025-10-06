from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class StockDailyData(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10)

    date: date
    open_price: float = Field(..., gt=0, description="Opening price")
    high_price: float = Field(..., gt=0, description="Highest price")
    low_price: float = Field(..., gt=0, description="Lowest price")
    close_price: float = Field(..., gt=0, description="Closing price")

    volume: int = Field(..., ge=0, description="Trading volume")

    daily_change_percentage: Optional[float] = None
    extraction_timestamp: datetime = Field(default_factory=datetime.now)

    @field_validator('high_price')
    @classmethod
    def validate_high_price(cls, high_price: float, info) -> float:
        data = info.data

        if 'low_price' in data and high_price < data['low_price']:
            raise ValueError(
                f"High price ({high_price}) cannot be less than "
                f"low price ({data['low_price']})"
            )

        return high_price

    @field_validator('low_price')
    @classmethod
    def validate_low_price(cls, low_price: float, info) -> float:
        data = info.data

        if 'high_price' in data and low_price > data['high_price']:
            raise ValueError(
                f"Low price ({low_price}) cannot be greater than "
                f"high price ({data['high_price']})"
            )

        return low_price

    def calculate_daily_change(self):
        if self.open_price > 0:
            self.daily_change_percentage = (
                                                   (self.close_price - self.open_price) / self.open_price
                                           ) * 100
        else:
            self.daily_change_percentage = 0.0

        return self

    class Config:
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat()
        }


class AlphaVantageResponse(BaseModel):
    meta_data: dict = Field(..., alias="Meta Data")

    time_series: dict = Field(..., alias="Time Series (Daily)")

    class Config:
        populate_by_name = True


