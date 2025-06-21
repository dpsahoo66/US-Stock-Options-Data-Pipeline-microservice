# OptionsData.py
from django.db import models

class OptionsData(models.Model):
    symbol = models.CharField(max_length=20)
    expiry_date = models.DateField()
    strike = models.FloatField()
    option_type = models.CharField(max_length=10)
    timestamp = models.DateTimeField()
    last_price = models.FloatField()

    class Meta:
        db_table = "options_data"
        unique_together = (('symbol', 'expiry_date', 'strike', 'option_type', 'timestamp'),)
