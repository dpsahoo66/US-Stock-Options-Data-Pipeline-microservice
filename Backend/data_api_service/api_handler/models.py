from django.db import models

class StockData(models.Model):
    id = models.AutoField(primary_key=True)
    symbol = models.CharField(max_length=10, null=True)
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    timestamp = models.DateTimeField(null=True)

    class Meta:
        db_table = 'StockData'
        app_label = 'api_handler'
        managed = False

# class StockData2(models.Model):
#     id = models.AutoField(primary_key=True)
#     symbol = models.CharField(max_length=10, null=True)
#     price = models.DecimalField(max_digits=10, decimal_places=2, null=True)
#     timestamp = models.DateTimeField(null=True)

#     class Meta:
#         db_table = 'StockData'
#         app_label = 'api_handler'
#         managed = False
#         database = 'secondary_db'