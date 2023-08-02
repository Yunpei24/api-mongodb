from pydantic import BaseModel


class Country(BaseModel):
    country: str
    rank: int
    area: float
    landAreaKm: float
    cca2: str
    cca3: str
    netChange: float
    growthRate: float
    worldPercentage: float
    density: float
    densityMi: float
    place: int
    pop1980: int
    pop2000: int
    pop2010: int
    pop2022: int
    pop2023: int
    pop2030: int
    pop2050: int
    
#generate a black kid in burkina faso color who bring a super-hero costum tech