from pathlib import Path

class Constants:
    config_path: Path = Path('config.ini')
    country_codes: list[str] = [
        'SVK'
        ,'CZE'
        ,'SVN'
        ,'LTU'
        ,'IRL'
        ,'HUN'
        ,'HRV'
        ,'LVA'
        ,'AUT'
        ,'BGR'
        ,'BEL'
        ,'SWE'
        ,'NOR'
        ,'DNK'
        ,'GRC'
        ,'FIN'
        ,'FRA'
        ,'POL'
        ,'ITA'
        ,'NLD'
        ,'GBR'
        ,'DEU'
        ,'CHE'
    ]
    default_daterange: str = "1991:2024"