from reportlab.lib import colors
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

OUT = r"C:\Dev\curso2526\AdvancedCpp\Mod2FuturesPromisesThreadPool\output\pdf\integrated-concurrency-flow.pdf"
W, H = (720 * mm, 270 * mm)

NAVY = colors.HexColor("#16324F")
BLUE = colors.HexColor("#DCEEFF")
GREEN = colors.HexColor("#DFF4E5")
AMBER = colors.HexColor("#FFF0C9")
PURPLE = colors.HexColor("#EEE5FF")
GREY = colors.HexColor("#F2F5F7")
INK = colors.HexColor("#1F2933")

def box(c, x, y, w, h, lines, fill=GREY, radius=8):
    c.setFillColor(fill)
    c.setStrokeColor(NAVY)
    c.setLineWidth(1.1)
    c.roundRect(x, y, w, h, radius, fill=1, stroke=1)
    c.setFillColor(INK)
    c.setFont("Helvetica-Bold", 10)
    leading = 13
    start = y + h / 2 + (len(lines) - 1) * leading / 2 - 3
    for i, line in enumerate(lines):
        c.drawCentredString(x + w / 2, start - i * leading, line)

def arrow(c, x1, y1, x2, y2, label=None, dashed=False):
    c.saveState()
    c.setStrokeColor(NAVY)
    c.setFillColor(NAVY)
    c.setLineWidth(1.3)
    if dashed:
        c.setDash(4, 3)
    c.line(x1, y1, x2, y2)
    import math
    a = math.atan2(y2-y1, x2-x1)
    s = 7
    c.line(x2, y2, x2 - s * math.cos(a - 0.45), y2 - s * math.sin(a - 0.45))
    c.line(x2, y2, x2 - s * math.cos(a + 0.45), y2 - s * math.sin(a + 0.45))
    c.restoreState()
    if label:
        c.setFillColor(NAVY)
        c.setFont("Helvetica", 8)
        c.drawCentredString((x1+x2)/2, (y1+y2)/2 + 4, label)

def diamond(c, x, y, w, h, lines):
    p = c.beginPath()
    p.moveTo(x + w/2, y + h)
    p.lineTo(x + w, y + h/2)
    p.lineTo(x + w/2, y)
    p.lineTo(x, y + h/2)
    p.close()
    c.setFillColor(AMBER); c.setStrokeColor(NAVY); c.setLineWidth(1.1)
    c.drawPath(p, fill=1, stroke=1)
    c.setFillColor(INK); c.setFont("Helvetica-Bold", 9)
    for i, line in enumerate(lines):
        c.drawCentredString(x + w/2, y + h/2 + 4 - i*11, line)

c = canvas.Canvas(OUT, pagesize=(W, H))
c.setTitle("IntegratedConcurrencyArchitecture - flujo general")
c.setFillColor(NAVY)
c.setFont("Helvetica-Bold", 22)
c.drawString(24*mm, H - 24*mm, "IntegratedConcurrencyArchitecture")
c.setFillColor(INK)
c.setFont("Helvetica", 11)
c.drawString(24*mm, H - 31*mm, "Flujo general de datos, tareas asincronas y ejecucion concurrente")

# Coordinates in a broad, left-to-right pipeline.
box(c, 25*mm, 175*mm, 45*mm, 20*mm, ["dataGenerator", "10.000 MarketTick"], BLUE)
box(c, 85*mm, 175*mm, 48*mm, 20*mm, ["ingestMarketData", "enqueue"], BLUE)
box(c, 150*mm, 165*mm, 55*mm, 40*mm, ["LockFreeQueue<MarketData>", "cola compartida", "sin mutex"], PURPLE)
box(c, 225*mm, 175*mm, 55*mm, 20*mm, ["processDataStream", "lotes de hasta 100"], GREEN)
box(c, 300*mm, 175*mm, 52*mm, 20*mm, ["processBatch", "std::visit"], GREEN)

arrow(c, 70*mm, 185*mm, 85*mm, 185*mm)
arrow(c, 133*mm, 185*mm, 150*mm, 185*mm)
arrow(c, 205*mm, 185*mm, 225*mm, 185*mm, "dequeue")
arrow(c, 280*mm, 185*mm, 300*mm, 185*mm)

box(c, 365*mm, 205*mm, 50*mm, 20*mm, ["MarketTick", "processMarketTick"], BLUE)
box(c, 365*mm, 145*mm, 50*mm, 20*mm, ["TradeSignal", "processTradeSignal"], BLUE)
arrow(c, 352*mm, 190*mm, 365*mm, 215*mm, "tick")
arrow(c, 352*mm, 180*mm, 365*mm, 155*mm, "signal")

box(c, 430*mm, 205*mm, 54*mm, 20*mm, ["latestPrices", "shared_mutex"], GREY)
diamond(c, 500*mm, 202*mm, 38*mm, 27*mm, ["Cambio", "> 5%?"])
box(c, 555*mm, 205*mm, 54*mm, 20*mm, ["AsyncTaskManager", "hilo detached"], PURPLE)
box(c, 625*mm, 205*mm, 55*mm, 20*mm, ["analyzeSignificantMove", "50 ms"], PURPLE)
arrow(c, 415*mm, 215*mm, 430*mm, 215*mm, "lee y actualiza")
arrow(c, 484*mm, 215*mm, 500*mm, 215*mm)
arrow(c, 538*mm, 215*mm, 555*mm, 215*mm, "si")
arrow(c, 609*mm, 215*mm, 625*mm, 215*mm)

box(c, 555*mm, 145*mm, 60*mm, 20*mm, ["DynamicThreadPool", "cola por prioridad"], GREEN)
box(c, 635*mm, 145*mm, 50*mm, 20*mm, ["executeTradeSignal", "10 ms"], GREEN)
arrow(c, 415*mm, 155*mm, 555*mm, 155*mm, "submit HIGH")
arrow(c, 615*mm, 155*mm, 635*mm, 155*mm)

# Periodic generator feedback.
box(c, 230*mm, 85*mm, 58*mm, 20*mm, ["generateTradingSignals", "cada 1 segundo"], AMBER)
box(c, 310*mm, 85*mm, 55*mm, 20*mm, ["lee latestPrices", "shared_lock"], AMBER)
diamond(c, 385*mm, 82*mm, 42*mm, 27*mm, ["Tick", "reciente?"])
box(c, 450*mm, 85*mm, 55*mm, 20*mm, ["generatePatternSignal", "HOLD"], AMBER)
box(c, 525*mm, 85*mm, 45*mm, 20*mm, ["ingestSignal", "enqueue"], AMBER)
arrow(c, 288*mm, 95*mm, 310*mm, 95*mm)
arrow(c, 365*mm, 95*mm, 385*mm, 95*mm)
arrow(c, 427*mm, 95*mm, 450*mm, 95*mm, "si")
arrow(c, 505*mm, 95*mm, 525*mm, 95*mm)
arrow(c, 525*mm, 85*mm, 195*mm, 165*mm, "vuelve a la cola")

# Start and monitoring.
box(c, 25*mm, 85*mm, 150*mm, 20*mm, ["Constructor: inicia processDataStream (CRITICAL) y generateTradingSignals (HIGH) en el pool"], GREY)
arrow(c, 175*mm, 95*mm, 230*mm, 95*mm)
arrow(c, 175*mm, 105*mm, 252*mm, 175*mm)
box(c, 625*mm, 85*mm, 60*mm, 20*mm, ["monitor", "cada 2 s: metricas"], GREY)
arrow(c, 655*mm, 105*mm, 585*mm, 145*mm, "estadisticas")

c.setFillColor(INK); c.setFont("Helvetica", 8)
c.drawString(25*mm, 35*mm, "Nota: el resultado de analyzeSignificantMove se guarda como future, pero en este codigo no se consume ni se reinyecta como TradeSignal.")
c.drawString(25*mm, 29*mm, "El pool crece cuando su cola supera aproximadamente dos tareas por hilo; el descenso de hilos esta pendiente de implementar.")
c.showPage(); c.save()
print(OUT)
