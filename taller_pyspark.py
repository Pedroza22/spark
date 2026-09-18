import os
import shutil
import random
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql.window import Window

# ============================================================
# PASO 0: CONFIGURACION
# ============================================================
OUTPUT_DIR = "resultados"
DATASET_CSV = "online_retail_sintetico.csv"
TARGET_FACTURAS = 2000
SEMILLA = 42

random.seed(SEMILLA)

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("TallerPySpark_OnlineRetail") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 70)
print("TALLER 1 - ETL con PySpark (Online Retail Dataset)")
print("=" * 70)

# ============================================================
# PASO 1: GENERAR DATASET SINTETICO (simil UCI Online Retail)
# ============================================================
print("\n>>> PASO 1: Generando dataset sintetico ...")

PAISES = [
    "United Kingdom", "France", "Germany", "Spain", "Netherlands",
    "Belgium", "Switzerland", "Portugal", "Australia", "Italy",
    "Norway", "Japan", "Sweden", "Denmark", "Finland", "Austria",
    "Greece", "Singapore", "Hong Kong", "Iceland", "Canada", "Malta",
    "USA", "Czech Republic", "Lebanon", "United Arab Emirates",
    "Israel", "Saudi Arabia", "Brazil", "Bahrain", "Lithuania"
]

PRODUCTOS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053", "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.39),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART.", 3.39),
    ("22752", "SET 7 BABUSHKA NESTING BOXES", 7.65),
    ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ("22633", "HAND WARMER UNION JACK", 1.85),
    ("22632", "HAND WARMER RED POLKA DOT", 1.85),
    ("82482", "WOODEN PICTURE FRAME WHITE FINISH", 2.95),
    ("22960", "JAM MAKING SET WITH JARS", 4.25),
    ("22961", "JAM MAKING SET PRINTED", 3.45),
    ("22114", "HOT WATER BOTTLE TEA AND SYMPATHY", 3.95),
    ("21033", "JUMBO BAG CHARLIE+LOLA", 4.21),
    ("21035", "JUMBO BAG VINTAGE RED", 2.10),
    ("21068", "JUMBO BAG PINK VINTAGE PAISLEY", 4.21),
    ("21071", "JUMBO BAG 50'S CHRISTMAS", 2.10),
    ("20725", "LUNCH BAG RED RETROSPOT", 1.65),
    ("20726", "LUNCH BAG WOODLAND", 1.65),
    ("20727", "LUNCH BAG  BLACK SKULL.", 1.65),
    ("20728", "LUNCH BAG CARS BLUE", 1.65),
    ("20749", "WOODLAND CHARLOTTE BAG", 0.85),
    ("20750", "RED RETROSPOT CHARLOTTE BAG", 0.85),
    ("20751", "SKULLS AND CROSSBONES CHARLOTTE BAG", 0.85),
    ("20752", "BLUE RINSE CHARLOTTE BAG", 0.85),
    ("20753", "LUNCH BAG APPLE DESIGN", 1.65),
    ("20754", "RETROSPOT GIANT TUBE MATCHES", 4.95),
    ("20755", "CHRISTMAS GARLAND STARS,TREES", 4.95),
    ("20756", "BELL HEART GLASS JEWEL STAND", 4.95),
    ("20757", "DAISY HEART GLASS JEWEL STAND", 4.95),
    ("20758", "HEART MEASURING SPOONS LARGE", 4.95),
    ("20759", "MEASURING TAPE BEAUTY GIRL", 1.65),
    ("20760", "MEASURING TAPE SUKI", 1.65),
    ("20761", "TOAST ITS - I LOVE LONDON", 1.25),
    ("20762", "TOAST ITS - SAVE THE PLANET", 1.25),
    ("20763", "TOAST ITS - PEACE AND LOVE", 1.25),
    ("20764", "TOAST ITS - 2010 CALENDAR", 1.25),
    ("20765", "MEASURING TAPE COURTYARD", 1.65),
    ("20766", "MONEY BOX 3D METAL CHRISTMAS", 5.95),
    ("20767", "MONEY BOX 3D METAL SANTAS GROTTO", 5.95),
    ("20768", "MONEY BOX 3D METAL REINDEER", 5.95),
    ("20769", "MONEY BOX 3D METAL ROBIN", 5.95),
    ("20770", "PACK OF 72 RETRO SPOT CAKE CASES", 0.85),
    ("20771", "RETROSPOT PARTY BAG + STICKER SHEET", 2.10),
    ("20772", "SWEDISH POSTMARK MUG", 1.06),
    ("20773", "MIDNIGHT BLUE GLASS JAR, SMALL", 4.95),
    ("20774", "MIDNIGHT BLUE GLASS JAR, MEDIUM", 6.75),
    ("20775", "MIDNIGHT BLUE GLASS JAR, LARGE", 9.95),
    ("20776", "HAND PAINTED MAGNET GARDENERS SET", 2.10),
    ("20777", "BISCUIT TIN 60'S LOVE HEART", 10.75),
    ("20778", "MIRRORED WALL ART JEWELLERY STAND", 32.95),
    ("20779", "3D HEARTS HONEYCOMB PAPER GARLAND", 1.65),
    ("20780", "3D STARS HONEYCOMB PAPER GARLAND", 1.65),
    ("20781", "3D BELLS HONEYCOMB PAPER GARLAND", 1.65),
    ("20782", "PAINTED METAL HEART, HOOK", 1.65),
    ("20783", "PAINTED METAL STAR, HOOK", 1.65),
    ("20784", "PAINTED METAL BELL, HOOK", 1.65),
    ("20785", "SMALL RED RETROSPOT WOVEN BAG", 3.37),
    ("20786", "SMALL PURPLE, WHITE BUTTERFLY BAG", 3.37),
    ("20787", "SMALL PINK CREAM BUTTERFLY BAG", 3.37),
    ("20788", "SMALL RED BUTTERFLY BAG", 3.37),
    ("20789", "SMALL GREEN, BLUE BUTTERFLY BAG", 3.37),
    ("20790", "SMALL RED WOVEN BAG", 3.37),
    ("20791", "SMALL IVORY WOVEN BAG", 3.37),
    ("20792", "SMALL YELLOW WOVEN BAG", 3.37),
    ("20793", "SMALL TURQUOISE WOVEN BAG", 3.37),
    ("20794", "SMALL RED, WHITE BUTTERFLY BAG", 3.37),
    ("20795", "SMALL HEART MEASURING SPOONS", 2.10),
    ("20796", "SMALL STAR MEASURING SPOONS", 2.10),
    ("20797", "SILVER 'LOVE' METAL BOOKMARK", 3.37),
    ("20798", "SILVER M.O.P. ORNAMENT -  BUTTERFLY", 4.21),
    ("20799", "SILVER M.O.P. ORNAMENT - SNOWFLAKE", 4.21),
    ("20800", "SILVER M.O.P. ORNAMENT - STAR", 4.21),
    ("20801", "SMALL PINK BUFFALO CHECK CASE", 3.37),
    ("20802", "SMALL CHECKERED CASE", 3.37),
    ("20803", "SMALL DOLLY MIX DESIGN ORANGE BOWL", 1.65),
    ("20804", "SMALL DOLLY MIX DESIGN PINK BOWL", 1.65),
    ("20805", "SMALL DOLLY MIX DESIGN RED BOWL", 1.65),
    ("20806", "SMALL DOLLY MIX DESIGN YELLOW BOWL", 1.65),
    ("20807", "WOODLAND LUGGAGE TAG", 1.65),
    ("20808", "RETROSPOT LUGGAGE TAG", 1.65),
    ("20809", "BLACK SKULL LUGGAGE TAG", 1.65),
    ("20810", "CARS LUGGAGE TAG", 1.65),
    ("20811", "SUKI LUGGAGE TAG", 1.65),
    ("20812", "COURTYARD LUGGAGE TAG", 1.65),
    ("20813", "3D HEART DECORATION RIBBON", 1.65),
    ("20814", "3D STAR DECORATION RIBBON", 1.65),
    ("20815", "3D BELL DECORATION RIBBON", 1.65),
    ("20816", "LARGE HEART MEASURING SPOONS", 2.55),
    ("20817", "LARGE STAR MEASURING SPOONS", 2.55),
    ("20818", "GOLD MUG, BONE CHINA", 12.35),
    ("20819", "SILVER ROCCOCO CHANDELIER", 10.95),
    ("20820", "TUMBLER, NEW ENGLAND", 2.55),
    ("20821", "SILVER GLITTER STAR GARLAND", 1.25),
    ("20822", "GOLD GLITTER STAR GARLAND", 1.25),
    ("20823", "GOLD DOTTIE TAPE", 1.25),
    ("20824", "PINK GOLD TAPE", 1.25),
    ("20825", "SILVER DOTTIE TAPE", 1.25),
    ("20826", "METAL SIGN,CUPCAKE SINGLE HOOK", 1.65),
    ("20827", "METAL SIGN,DINER WALL ART", 1.65),
    ("20828", "METAL SIGN,SHELLS WALL ART", 1.65),
    ("20829", "SILVER 'I LOVE YOU' METAL BOOKMARK", 3.37),
    ("20830", "SILVER AND BLACK GLASS T-LIGHT HOLDER", 5.79),
    ("20831", "JAM JAR WITH GREEN LID", 3.75),
    ("20832", "JAM JAR WITH PINK LID", 3.75),
    ("20833", "JAM JAR WITH BLUE LID", 3.75),
    ("20834", "JAM JAR WITH YELLOW LID", 3.75),
    ("20835", "JAM JAR WITH RED LID", 3.75),
    ("20836", "JAM JAR WITH WHITE LID", 3.75),
    ("20837", "SAUCER, PINK AND BLUE SPOTS", 0.85),
    ("20838", "SAUCER, RED AND WHITE SPOTS", 0.85),
    ("20839", "SAUCER, GREEN AND YELLOW SPOTS", 0.85),
    ("20840", "SAUCER, ORANGE AND LILAC SPOTS", 0.85),
    ("20841", "SAUCER, MULTI HEIGHTS", 0.85),
    ("20842", "MUG, PINK AND BLUE SPOTS", 1.06),
    ("20843", "MUG, RED AND WHITE SPOTS", 1.06),
    ("20844", "MUG, GREEN AND YELLOW SPOTS", 1.06),
    ("20845", "MUG, ORANGE AND LILAC SPOTS", 1.06),
    ("20846", "MUG, MULTI HEIGHTS", 1.06),
    ("20847", "CAKE STAND, WHITE, 2 TIERS", 12.75),
    ("20848", "CAKE STAND, WHITE, 3 TIERS", 16.95),
    ("20849", "CAKE PLATE, WHITE, 20 CM", 7.95),
    ("20850", "CAKE PLATE, WHITE, 25 CM", 10.75),
    ("20851", "LARGE CAKE TOWEL, CHOCOLATE SPOTS", 1.06),
    ("20852", "LARGE CAKE TOWEL, PINK  POLKADOT", 1.06),
    ("20853", "LARGE CAKE TOWEL, BLUE POLKADOT", 1.06),
    ("20854", "CAKE TOWEL, CHOCOLATE SPOTS", 0.85),
    ("20855", "CAKE TOWEL, PINK  POLKADOT", 0.85),
    ("20856", "CAKE TOWEL, BLUE POLKADOT", 0.85),
    ("20857", "TOXIC AREA  DOOR HANGER", 1.25),
    ("20858", "COOL WIZARD DOOR HANGER", 1.25),
    ("20859", "OUT OF ORDER DOOR HANGER", 1.25),
    ("20860", "WORLD WAR 2 GLIDERS ASSTD DESIGNS", 3.35),
    ("20861", "BAKING MOULD ROSE FAIRY CAKE", 8.29),
    ("20862", "BAKING MOULD CHIMNEY POT", 10.75),
    ("20863", "BAKING MOULD CHRISTMAS TREE", 8.29),
    ("20864", "BAKING MOULD CUPCAKE CHOCOLATE", 8.29),
    ("20865", "BAKING MOULD CUPCAKE VANILLA", 8.29),
    ("20866", "ROBIN CHRISTMAS CARD", 1.25),
    ("20867", "CHRISTMAS CARD STACKED PRESENTS", 1.25),
    ("20868", "CHRISTMAS CARD STAG HEADS", 1.25),
    ("20869", "CHRISTMAS CARD REINDEER CARRIAGE", 1.25),
    ("20870", "CHRISTMAS CARD COLOURED SNOWMAN", 1.25),
    ("20871", "CHRISTMAS CARD RAINDEER AND SLEIGH", 1.25),
    ("20872", "CHRISTMAS CARD, ROBIN BALANCING", 0.42),
    ("20873", "FIRST CLASS HOLIDAY CARD, PACK OF 8", 1.45),
    ("20874", "RAINBOW HOLIDAY CARD, PACK OF 8", 1.45),
    ("20875", "PENGUIN HOLIDAY CARD, PACK OF 8", 1.45),
    ("20876", "MERRY CHRISTMAS HOLIDAY CARD, 8 PACK", 1.45),
    ("20877", "CHRISTMAS TREE HOLIDAY CARD, 8 PACK", 1.45),
    ("20878", "GREEN REINDEER CHRISTMAS CARD, 8 PK", 1.45),
    ("20879", "RED RETROSPOT WRAP", 1.25),
    ("20880", "WOODLAND WRAP", 1.25),
    ("20881", "SKULLS AND CROSSBONES WRAP", 1.25),
    ("20882", "CHRISTMAS GROTTO WRAP", 1.25),
    ("20883", "VINTAGE PAISLEY WRAP", 1.25),
    ("20884", "WRAP, BLUE POLKADOT", 0.42),
    ("20885", "WRAP, RED POLKADOT", 0.42),
    ("20886", "WRAP, PINK POLKADOT", 0.42),
    ("20887", "WRAP, GREEN POLKADOT", 0.42),
    ("20888", "WRAP, SUKI", 0.42),
    ("20889", "WRAP, WOODLAND", 0.42),
    ("20890", "WRAP, RED RETROSPOT", 0.42),
    ("20891", "WRAP, PINK  POLKADOT", 0.42),
    ("20892", "WRAP, BLUE PAISLEY", 0.42),
    ("20893", "WRAP, PINK PAISLEY", 0.42),
    ("20894", "WRAP, GREEN PAISLEY", 0.42),
    ("20895", "WRAP, 50'S CHRISTMAS", 0.42),
    ("20896", "WRAP, 60'S CHRISTMAS", 0.42),
    ("20897", "WRAP, 70'S CHRISTMAS", 0.42),
    ("20898", "WRAP, 80'S CHRISTMAS", 0.42),
    ("20899", "WRAP, TRADITIONAL WOODLAND", 0.42),
    ("20900", "STRAWBERRIES WRAP, DESIGN 2", 0.42),
    ("20901", "ALPHABET HEARTS STICKER SHEET", 0.85),
    ("20902", "ALPHABET FAIRY CAKES STICKER SHEET", 0.85),
    ("20903", "ALPHABET ROCKETS STICKER SHEET", 0.85),
    ("20904", "FELT EASTER CHICK GARLAND", 1.65),
    ("20905", "FELT EASTER BUNNY GARLAND", 1.65),
    ("20906", "FELT FLOWERS GARLAND", 1.65),
    ("20907", "CHRISTMAS TREE DECORATION, ANGEL", 2.10),
    ("20908", "CHRISTMAS TREE DECORATION, STAR", 2.10),
    ("20909", "CHRISTMAS TREE DECORATION, BELL", 2.10),
    ("20910", "CHRISTMAS TREE DECORATION, HEART", 2.10),
    ("20911", "CHRISTMAS TREE DECORATION, SNOWMAN", 2.10),
    ("20912", "CHRISTMAS TREE DECORATION, REINDEER", 2.10),
    ("20913", "CHRISTMAS TREE DECORATION, SANTA", 2.10),
    ("20914", "SET/5 RED RETROSPOT LID GLASS BOWLS", 4.95),
    ("20915", "SET/5 PINK  POLKADOT GLASS BOWLS", 4.95),
    ("20916", "SET/5 BLUE POLKADOT LID GLASS BOWLS", 4.95),
    ("20917", "WOODEN TREE CHRISTMAS SCANDINAVIAN", 3.75),
    ("20918", "GLASS  SONGBIRD STORAGE JAR", 4.95),
    ("20919", "GLASS  HEART STORAGE JAR", 4.95),
    ("20920", "GLASS  STAR STORAGE JAR", 4.95),
    ("20921", "GLASS  BELL STORAGE JAR", 4.95),
    ("20922", "GLASS  MOON STORAGE JAR", 4.95),
    ("20923", "GLASS  SUN STORAGE JAR", 4.95),
    ("20924", "6 RIBBONS SHIMMERING PINKS", 2.21),
    ("20925", "6 RIBBONS SHIMMERING WHITES", 2.21),
    ("20926", "6 RIBBONS SHIMMERING GREYS", 2.21),
    ("20927", "6 RIBBONS SHIMMERING REDS", 2.21),
    ("20928", "6 RIBBONS SHIMMERING PASTELS", 2.21),
    ("20929", "6 RIBBONS SHIMMERING BRIGHTS", 2.21),
    ("20930", "PINK HEART SHAPE EGG FRYING PAN", 3.75),
    ("20931", "STAR SHAPE EGG FRYING PAN", 3.75),
    ("20932", "HEART SHAPE EGG FRYING PAN", 3.75),
    ("20933", "FLOWER SHAPE EGG FRYING PAN", 3.75),
    ("20934", "ROUND TIN, BUNNIES, PINK", 1.45),
    ("20935", "ROUND TIN, BUNNIES, PALE BLUE", 1.45),
    ("20936", "ROUND TIN, BUNNIES, CREAM", 1.45),
    ("20937", "ROUND TIN, VINTAGE LEAF", 1.45),
    ("20938", "ROUND TIN, 50'S CHRISTMAS", 1.45),
    ("20939", "ROUND TIN, 60'S CHRISTMAS", 1.45),
    ("20940", "ROUND TIN, 70'S CHRISTMAS", 1.45),
    ("20941", "ROUND TIN, TRADITIONAL WOODLAND", 1.45),
    ("20942", "HOT WATER BOTTLE KEEP CALM", 4.95),
    ("20943", "HOT WATER BOTTLE I AM SO POORLY", 4.95),
    ("20944", "HOT WATER BOTTLE TEA AND SYMPATHY", 4.95),
    ("20945", "HOT WATER BOTTLE KEEP CALM LOVE", 4.95),
    ("20946", "HOT WATER BOTTLE KEEP CALM WHITE", 4.95),
    ("20947", "HOT WATER BOTTLE PINK STAR DESIGN", 4.95),
    ("20948", "HOT WATER BOTTLE BLUE STAR DESIGN", 4.95),
    ("20949", "HOT WATER BOTTLE GREEN STAR DESIGN", 4.95),
    ("20950", "HOT WATER BOTTLE RED STAR DESIGN", 4.95),
    ("20951", "HOT WATER BOTTLE YELLOW STAR DESIGN", 4.95),
    ("20952", "JAM JAR NURSERY RHYME PINK", 0.85),
    ("20953", "JAM JAR NURSERY RHYME BLUE", 0.85),
    ("20954", "JAM JAR NURSERY RHYME YELLOW", 0.85),
    ("20955", "JAM JAR NURSERY RHYME GREEN", 0.85),
    ("20956", "JAM JAR NURSERY RHYME RED", 0.85),
    ("20957", "JAM JAR NURSERY RHYME ORANGE", 0.85),
    ("20958", "CHRISTMAS CARD, XMAS TREE LIGHTS", 0.42),
    ("20959", "CHRISTMAS CARD, HAPPY SANTA", 0.42),
    ("20960", "CHRISTMAS CARD, RUDOLPH RED NOSE", 0.42),
    ("20961", "STRAWBERRY CERAMIC TRINKET POT", 1.06),
    ("20962", "BLUE POLKADOT CERAMIC TRINKET POT", 1.06),
    ("20963", "PINK POLKADOT CERAMIC TRINKET POT", 1.06),
    ("20964", "GREEN POLKADOT CERAMIC TRINKET POT", 1.06),
    ("20965", "RED POLKADOT CERAMIC TRINKET POT", 1.06),
    ("20966", "SANDWICH BATH SPONGE", 1.06),
    ("20967", "GEORGY TREE HANGER SILVER", 1.65),
    ("20968", "GEORGY TREE HANGER GOLD", 1.65),
    ("20969", "GEORGY TREE HANGER, WOOD", 1.65),
    ("20970", "FOLK ART STAR CANDLEHOLDER", 1.65),
    ("20971", "FOLK ART HEART CANDLEHOLDER", 1.65),
    ("20972", "PINK CREAM FELT CRAFT TRINKET POT", 1.06),
    ("20973", "BLUE PINK FELT CRAFT TRINKET POT", 1.06),
    ("20974", "YELLOW PINK FELT CRAFT TRINKET POT", 1.06),
    ("20975", "GREEN PINK FELT CRAFT TRINKET POT", 1.06),
    ("20976", "SWEETHEART CERAMIC TRINKET BOX", 1.06),
    ("20977", "GREETING CARD,  HAPPY BIRTHDAY", 1.06),
    ("20978", "CARD, BILLBOARD FONT DESIGN", 1.06),
    ("20979", "CARD, VINTAGE BIRTHDAY COLLAGE", 1.06),
    ("20980", "CARD, 3D HEART HAPPY BIRTHDAY", 1.06),
    ("20981", "BOTANICAL GARDENS WALL ART", 10.75),
    ("20982", "MISTLETOE BOTANICAL WALL ART", 10.75),
    ("20983", "SCANDINAVIAN PAISLEY WRAP", 0.42),
    ("20984", "ROTATING SILVER ANGELS T-LIGHT HLDR", 12.35),
    ("20985", "HEART T-LIGHT HOLDER, SQUARE, SILVER", 3.37),
    ("20986", "STAR T-LIGHT HOLDER SILVER", 3.37),
    ("20987", "BELL T-LIGHT HOLDER SILVER", 3.37),
    ("20988", "JUMBO BAG ALPHABET, PAISLEY", 2.10),
    ("20989", "JUMBO BAG 50'S CHRISTMAS", 2.10),
    ("20990", "JUMBO BAG WOODLAND ANIMALS", 2.10),
    ("20991", "JUMBO BAG VINTAGE LEAF", 2.10),
    ("20992", "JUMBO BAG PEARS", 2.10),
    ("20993", "JUMBO BAG APPLES", 2.10),
    ("20994", "JUMBO BAG STRAWBERRY", 2.10),
    ("20995", "JUMBO BAG CHERRY BLOSSOM", 2.10),
    ("20996", "JUMBO BAG SCANDINAVIAN PAISLEY", 2.10),
    ("20997", "JUMBO BAG DOILEY PATTERNS", 2.10),
    ("20998", "JUMBO BAG SPACEBOY", 2.10),
    ("20999", "JUMBO BAG DINOSAUR", 2.10),
    ("21000", "JUMBO BAG RAINBOW", 2.10)
]

TOTAL_FACTURAS_INICIALES = 18000
NUM_CLIENTES = 3500
FECHA_INICIO = datetime(2010, 12, 1)
FECHA_FIN = datetime(2011, 12, 9)

registros = []
invoice_counter = 100000

for factura_idx in range(TOTAL_FACTURAS_INICIALES):
    invoice_no = str(invoice_counter + factura_idx)
    es_cancelacion = random.random() < 0.08
    if es_cancelacion:
        invoice_no = "C" + invoice_no

    fecha = FECHA_INICIO + timedelta(
        days=random.randint(0, (FECHA_FIN - FECHA_INICIO).days),
        hours=random.randint(6, 20),
        minutes=random.randint(0, 59)
    )
    customer_id = random.randint(12346, 12346 + NUM_CLIENTES - 1)
    pais = random.choices(
        PAISES,
        weights=[80] + [2] * 20 + [1] * 10,
        k=1
    )[0]

    num_lineas = random.randint(1, 35)
    productos_seleccionados = random.sample(PRODUCTOS, min(num_lineas, len(PRODUCTOS)))

    for stock_code, descripcion, precio_unitario in productos_seleccionados:
        if es_cancelacion:
            cantidad = -random.randint(1, 20)
        else:
            cantidad = random.choice(
                [1, 1, 1, 1, 2, 2, 2, 3, 3, 4, 5, 6, 8, 10, 12, 15, 20, 24, 30, 48, 72, 100]
            )

        if random.random() < 0.02:
            precio_final = 0.0
        else:
            variacion = random.uniform(0.85, 1.20)
            precio_final = round(precio_unitario * variacion, 2)

        registros.append({
            "InvoiceNo": invoice_no,
            "StockCode": stock_code,
            "Description": descripcion,
            "Quantity": cantidad,
            "InvoiceDate": fecha,
            "UnitPrice": precio_final,
            "CustomerID": customer_id,
            "Country": pais
        })

df_pandas = pd.DataFrame(registros)
df_pandas.to_csv(DATASET_CSV, index=False)
print(f"  Dataset generado: {len(df_pandas)} lineas, {df_pandas['InvoiceNo'].nunique()} facturas iniciales")

# ============================================================
# PASO 2: CARGA DE DATOS EN SPARK (Lectura CSV)
# ============================================================
print("\n>>> PASO 2: Cargando datos en PySpark ...")

schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True)
])

df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .schema(schema) \
    .load(DATASET_CSV)

facturas_iniciales = df_raw.select("InvoiceNo").distinct().count()
print(f"  Facturas cargadas en Spark: {facturas_iniciales}")
print(f"  Total de lineas: {df_raw.count()}")

# ============================================================
# PASO 3: APLICAR FILTRADOS SUCESIVOS HASTA 2000 FACTURAS
# ============================================================
print("\n>>> PASO 3: Aplicando filtrados sucesivos ...")
print(f"  Objetivo: {TARGET_FACTURAS} facturas")

# --- FILTRADO 1: Eliminar facturas de cancelacion (empiezan por C) ---
print(f"\n  [FILTRO 1] Eliminar facturas de cancelacion (InvoiceNo empieza por C)")
print(f"    Antes: {df_raw.select('InvoiceNo').distinct().count()} facturas")

df_f1 = df_raw.filter(~F.col("InvoiceNo").startswith("C"))
f1_count = df_f1.select("InvoiceNo").distinct().count()
print(f"    Despues: {f1_count} facturas")

# --- FILTRADO 2: Eliminar lineas con Quantity negativo o cero ---
print(f"\n  [FILTRO 2] Eliminar lineas con Quantity <= 0")
print(f"    Antes: {df_f1.select('InvoiceNo').distinct().count()} facturas")

df_f2 = df_f1.filter(F.col("Quantity") > 0)
f2_count = df_f2.select("InvoiceNo").distinct().count()
print(f"    Despues: {f2_count} facturas")

# --- FILTRADO 3: Eliminar lineas con UnitPrice negativo ---
print(f"\n  [FILTRO 3] Eliminar lineas con UnitPrice <= 0 (productos gratuitos)")
print(f"    Antes: {df_f2.select('InvoiceNo').distinct().count()} facturas")

df_f3 = df_f2.filter(F.col("UnitPrice") > 0)
f3_count = df_f3.select("InvoiceNo").distinct().count()
print(f"    Despues: {f3_count} facturas")

# --- FILTRADO 4: Eliminar facturas sin CustomerID (nulos) ---
print(f"\n  [FILTRO 4] Eliminar facturas sin CustomerID valido")
print(f"    Antes: {df_f3.select('InvoiceNo').distinct().count()} facturas")

df_f4 = df_f3.filter(F.col("CustomerID").isNotNull())
f4_count = df_f4.select("InvoiceNo").distinct().count()
print(f"    Despues: {f4_count} facturas")

# --- FILTRADO 5: Seleccionar solo facturas dentro del rango completo 2011 ---
print(f"\n  [FILTRO 5] Filtrar por rango de fechas: 01-Jan-2011 a 31-Oct-2011")
print(f"    Antes: {df_f4.select('InvoiceNo').distinct().count()} facturas")

df_f5 = df_f4.filter(
    (F.col("InvoiceDate") >= "2011-01-01 00:00:00") &
    (F.col("InvoiceDate") <= "2011-10-31 23:59:59")
)
f5_count = df_f5.select("InvoiceNo").distinct().count()
print(f"    Despues: {f5_count} facturas")

# --- FILTRADO 6: Eliminar productos extremadamente baratos (UnitPrice < 0.50) ---
print(f"\n  [FILTRO 6] Eliminar lineas con UnitPrice < 0.50 (muy baratos)")
print(f"    Antes: {df_f5.select('InvoiceNo').distinct().count()} facturas")

df_f6 = df_f5.filter(F.col("UnitPrice") >= 0.50)
f6_count = df_f6.select("InvoiceNo").distinct().count()
print(f"    Despues: {f6_count} facturas")

# --- FILTRADO 7: Mantener solo lineas con Quantity entre 2 y 100 ---
print(f"\n  [FILTRO 7] Mantener solo lineas con Quantity entre 2 y 100 (inclusivo)")
print(f"    Antes: {df_f6.select('InvoiceNo').distinct().count()} facturas")

df_f7 = df_f6.filter((F.col("Quantity") >= 2) & (F.col("Quantity") <= 100))
f7_count = df_f7.select("InvoiceNo").distinct().count()
print(f"    Despues: {f7_count} facturas")

# --- FILTRADO 8: Eliminar facturas que solo tienen una unica linea ---
# (Primero contamos lineas por factura, luego filtramos aquellas con >= 2 lineas)
print(f"\n  [FILTRO 8] Eliminar facturas con solo 1 linea de detalle (>= 2 lineas)")
print(f"    Antes: {f7_count} facturas")

lineas_por_factura = df_f7.groupBy("InvoiceNo").agg(F.count("*").alias("NumLineas"))
facturas_multilinea = lineas_por_factura.filter(F.col("NumLineas") >= 2).select("InvoiceNo")

df_f8 = df_f7.join(
    F.broadcast(facturas_multilinea),
    on="InvoiceNo",
    how="inner"
)
f8_count = df_f8.select("InvoiceNo").distinct().count()
print(f"    Despues: {f8_count} facturas")

# --- FILTRADO 9: Ajuste final para llegar exactamente a 2000 facturas ---
# Tomamos las primeras 2000 facturas ordenadas por fecha
if f8_count > TARGET_FACTURAS:
    print(f"\n  [FILTRO 9] Reduccion final: tomar primeras {TARGET_FACTURAS} facturas (ordenadas por fecha)")
    print(f"    Antes: {f8_count} facturas")

    facturas_seleccionadas = df_f8.select("InvoiceNo", "InvoiceDate") \
        .distinct() \
        .orderBy("InvoiceDate") \
        .limit(TARGET_FACTURAS) \
        .select("InvoiceNo")

    df_filtrado_final = df_f8.join(
        F.broadcast(facturas_seleccionadas),
        on="InvoiceNo",
        how="inner"
    )
else:
    print(f"\n  [!] No se alcanzaron {TARGET_FACTURAS} facturas, se usan las disponibles: {f8_count}")
    df_filtrado_final = df_f8

facturas_finales = df_filtrado_final.select("InvoiceNo").distinct().count()
lineas_finales = df_filtrado_final.count()

print(f"\n  >>> RESULTADO FINAL DE FILTRADOS <<<")
print(f"  Facturas: {facturas_finales}")
print(f"  Lineas de detalle: {lineas_finales}")

# --- Aplicar SELECT para demostrar operacion de seleccion ---
df_filtrado_final = df_filtrado_final.select(
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country"
)

# --- ORDER BY demostracion ---
df_filtrado_final = df_filtrado_final.orderBy("InvoiceDate", "InvoiceNo", "StockCode")

# ============================================================
# PASO 4: CREAR COLUMNAS DERIVADAS (withColumn)
# ============================================================
print("\n>>> PASO 4: Creando columnas derivadas ...")

df_transformado = df_filtrado_final \
    .withColumn("TotalLinea", F.round(F.col("Quantity") * F.col("UnitPrice"), 2)) \
    .withColumn("Anio", F.year(F.col("InvoiceDate"))) \
    .withColumn("Mes", F.month(F.col("InvoiceDate"))) \
    .withColumn("NombreMes", F.date_format(F.col("InvoiceDate"), "MMMM")) \
    .withColumn("DiaSemana", F.date_format(F.col("InvoiceDate"), "EEEE")) \
    .withColumn("EsFinDeSemana", F.when(
        F.dayofweek(F.col("InvoiceDate")).isin([1, 7]), F.lit(1)
    ).otherwise(F.lit(0))) \
    .withColumn("RangoPrecio", F.when(F.col("UnitPrice") < 1, "Muy Barato")
        .when(F.col("UnitPrice") < 5, "Barato")
        .when(F.col("UnitPrice") < 20, "Medio")
        .otherwise("Caro"))

print("  Columnas creadas: TotalLinea, Anio, Mes, NombreMes, DiaSemana, EsFinDeSemana, RangoPrecio")

# Persistimos para rendimiento
df_transformado.cache()
df_transformado.count()

# ============================================================
# PASO 5: RESPONDER LAS 10 PREGUNTAS DEL TALLER
# ============================================================
print("\n" + "=" * 70)
print("RESPUESTAS A LAS PREGUNTAS DEL TALLER")
print("=" * 70)

# ---------- PREGUNTA 1: Numero total de facturas ----------
print("\n[PREGUNTA 1] Cual es el numero total de facturas en el dataset?")
total_facturas = df_transformado.select("InvoiceNo").distinct().count()
df_p1 = spark.createDataFrame([(total_facturas,)], ["Total_Facturas"])
df_p1.show()
df_p1.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta01_total_facturas.csv", index=False)

# ---------- PREGUNTA 2: Numero de clientes unicos ----------
print("\n[PREGUNTA 2] Cual es el numero de clientes unicos?")
total_clientes = df_transformado.select("CustomerID").distinct().count()
df_p2 = spark.createDataFrame([(total_clientes,)], ["Clientes_Unicos"])
df_p2.show()
df_p2.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta02_clientes_unicos.csv", index=False)

# ---------- PREGUNTA 3: Ingreso total (Quantity * UnitPrice) ----------
print("\n[PREGUNTA 3] Cual es el ingreso total (Quantity * UnitPrice)?")
df_p3 = df_transformado.agg(
    F.round(F.sum(F.col("TotalLinea")), 2).alias("Ingreso_Total")
)
df_p3.show()
df_p3.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta03_ingreso_total.csv", index=False)

# ---------- PREGUNTA 4: Producto mas vendido en cantidad ----------
print("\n[PREGUNTA 4] Que producto fue el mas vendido en cantidad?")
df_p4 = df_transformado.groupBy("StockCode", "Description") \
    .agg(F.sum("Quantity").alias("Cantidad_Total")) \
    .orderBy(F.desc("Cantidad_Total")) \
    .limit(1)
df_p4.show(truncate=False)
df_p4.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta04_producto_mas_vendido.csv", index=False)

# ---------- PREGUNTA 5: Cliente con mayor volumen de compra en dinero ----------
print("\n[PREGUNTA 5] Cual es el cliente con mayor volumen de compra en dinero?")
df_p5 = df_transformado.groupBy("CustomerID") \
    .agg(F.round(F.sum("TotalLinea"), 2).alias("Total_Compras")) \
    .orderBy(F.desc("Total_Compras")) \
    .limit(1)
df_p5.show()
df_p5.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta05_cliente_mayor_compra.csv", index=False)

# ---------- PREGUNTA 6: Top 5 paises fuera de Reino Unido ----------
print("\n[PREGUNTA 6] Cuales son los 5 paises que mas compran fuera de Reino Unido?")
df_p6 = df_transformado.filter(F.col("Country") != "United Kingdom").groupBy("Country") \
    .agg(
        F.countDistinct("InvoiceNo").alias("Numero_Facturas"),
        F.round(F.sum("TotalLinea"), 2).alias("Ingreso_Total_Pais")
    ) \
    .orderBy(F.desc("Ingreso_Total_Pais")) \
    .limit(5)
df_p6.show(truncate=False)
df_p6.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta06_top5_paises_fuera_uk.csv", index=False)

# ---------- PREGUNTA 7: Ticket promedio por factura ----------
print("\n[PREGUNTA 7] Cual es el ticket promedio por factura?")
df_ticket_por_factura = df_transformado.groupBy("InvoiceNo") \
    .agg(F.round(F.sum("TotalLinea"), 2).alias("Total_Factura"))

df_p7 = df_ticket_por_factura.agg(
    F.round(F.avg("Total_Factura"), 2).alias("Ticket_Promedio")
)
df_p7.show()
df_p7.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta07_ticket_promedio.csv", index=False)

# ---------- PREGUNTA 8: Min, Max y Promedio de productos por factura ----------
print("\n[PREGUNTA 8] Cual es el minimo, maximo y promedio de productos por factura?")
df_productos_por_factura = df_transformado.groupBy("InvoiceNo") \
    .agg(
        F.sum("Quantity").alias("Unidades_Totales"),
        F.count("StockCode").alias("Lineas_Distintas")
    )

df_p8 = df_productos_por_factura.agg(
    F.min("Lineas_Distintas").alias("Min_Productos"),
    F.max("Lineas_Distintas").alias("Max_Productos"),
    F.round(F.avg("Lineas_Distintas"), 2).alias("Promedio_Productos")
)
df_p8.show()
df_p8.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta08_productos_por_factura.csv", index=False)

# ---------- PREGUNTA 9: Mes con mas ventas ----------
print("\n[PREGUNTA 9] Que mes del anio tuvo mas ventas?")
df_p9 = df_transformado.groupBy("Mes", "NombreMes") \
    .agg(F.round(F.sum("TotalLinea"), 2).alias("Ventas_Mes")) \
    .orderBy(F.desc("Ventas_Mes")) \
    .limit(1)
df_p9.show()
df_p9.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta09_mes_mas_ventas.csv", index=False)

# ---------- PREGUNTA 10: Porcentaje de facturas con devoluciones (lineas negativas) ----------
# Como filtramos cantidades negativas, calculamos sobre el dataset ANTES de los filtrados
print("\n[PREGUNTA 10] Cual es el porcentaje de facturas con devoluciones (cantidades negativas)?")

# Marcamos las facturas que tienen ALGUNA linea negativa en el dataset original
facturas_todas = df_raw.select("InvoiceNo").distinct().count()
facturas_con_negativos = df_raw.filter(F.col("Quantity") < 0) \
    .select("InvoiceNo") \
    .distinct() \
    .count()
porcentaje = round((facturas_con_negativos / facturas_todas) * 100, 2)

df_p10 = spark.createDataFrame(
    [(facturas_todas, facturas_con_negativos, porcentaje,)],
    ["Total_Facturas_Dataset", "Facturas_Con_Devoluciones", "Porcentaje_Devoluciones_Pct"]
)
df_p10.show()
df_p10.toPandas().to_csv(f"{OUTPUT_DIR}/pregunta10_porcentaje_devoluciones.csv", index=False)

# ============================================================
# PASO 6: CREAR UNIONES (JOINS) ENTRE DATAFRAMES
# ============================================================
print("\n>>> PASO 6: Demostracion de Join entre DataFrames ...")

df_resumen_cliente = df_transformado.groupBy("CustomerID") \
    .agg(
        F.countDistinct("InvoiceNo").alias("Num_Facturas"),
        F.round(F.sum("TotalLinea"), 2).alias("Total_Gastado"),
        F.round(F.avg("TotalLinea"), 2).alias("Ticket_Medio_Cliente")
    )

df_cliente_pais = df_transformado.select("CustomerID", "Country") \
    .distinct()

df_join_clientes = df_resumen_cliente.join(
    df_cliente_pais,
    on="CustomerID",
    how="inner"
).orderBy(F.desc("Total_Gastado"))

print("  Join entre resumen de cliente y paises (primeras 10 filas):")
df_join_clientes.limit(10).show(truncate=False)
df_join_clientes.limit(100).toPandas().to_csv(f"{OUTPUT_DIR}/join_clientes_resumen.csv", index=False)

# ============================================================
# PASO 7: FUNCIONES DE VENTANA (Window Functions)
# ============================================================
print("\n>>> PASO 7: Demostracion de Funciones de Ventana (Window) ...")

window_pais = Window.partitionBy("Country").orderBy(F.desc("TotalGastado"))

df_gasto_cliente_pais = df_transformado.groupBy("Country", "CustomerID") \
    .agg(F.round(F.sum("TotalLinea"), 2).alias("TotalGastado"))

df_rank_clientes = df_gasto_cliente_pais \
    .withColumn("Ranking_Pais", F.rank().over(window_pais)) \
    .withColumn("RowNum_Pais", F.row_number().over(window_pais)) \
    .filter(F.col("RowNum_Pais") <= 3) \
    .orderBy("Country", "RowNum_Pais")

print("  Top 3 clientes por pais (Window Functions rank/row_number):")
df_rank_clientes.show(20, truncate=False)
df_rank_clientes.toPandas().to_csv(f"{OUTPUT_DIR}/window_top3_clientes_por_pais.csv", index=False)

# ============================================================
# PASO 8: EXPORTACION DEL DATASET FILTRADO FINAL
# ============================================================
print("\n>>> PASO 8: Exportando resultados ...")

df_exportar = df_transformado.select(
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "UnitPrice", "TotalLinea", "InvoiceDate", "Anio", "Mes",
    "NombreMes", "DiaSemana", "EsFinDeSemana", "RangoPrecio",
    "CustomerID", "Country"
)

df_exportar_pd = df_exportar.toPandas()
df_exportar_pd.to_csv(f"{OUTPUT_DIR}/dataset_filtrado_2000_facturas.csv", index=False)
print(f"  Dataset final exportado: {len(df_exportar_pd)} lineas, {facturas_finales} facturas")

# ============================================================
# RESUMEN FINAL
# ============================================================
print("\n" + "=" * 70)
print("RESUMEN FINAL DEL TALLER")
print("=" * 70)
print(f"  Dataset inicial:       {facturas_iniciales:>6} facturas")
print(f"  Dataset filtrado:      {facturas_finales:>6} facturas (objetivo {TARGET_FACTURAS})")
print(f"  Lineas de detalle:     {lineas_finales:>6}")
print(f"  Clientes unicos:       {total_clientes:>6}")
print(f"  Ingreso total:         {df_p3.collect()[0][0]:>12.2f}")
print(f"  Preguntas CSV:         10 archivos generados")
print(f"  Carpeta de resultados: {OUTPUT_DIR}/")
print("=" * 70)

df_transformado.unpersist()
spark.stop()
print("\nProceso completado exitosamente!")
