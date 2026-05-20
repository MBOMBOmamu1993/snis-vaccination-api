import json, math
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrow, Rectangle, Patch
from matplotlib.lines import Line2D
from shapely.geometry import shape, Polygon, MultiPolygon, box
from shapely.ops import unary_union

T = json.load(open("/tmp/rdc_zs.topojson"))
obj = T["objects"]["Zone de SantéRDC"]
tr = T["transform"]
sx, sy = tr["scale"]; tx, ty = tr["translate"]
ARCS = T["arcs"]

def decode_arc(arc):
    pts = []
    x = y = 0
    for dx, dy in arc:
        x += dx; y += dy
        pts.append((x * sx + tx, y * sy + ty))
    return pts

DEC = [decode_arc(a) for a in ARCS]

def arc_coords(i):
    if i >= 0:
        return DEC[i]
    return DEC[~i][::-1]

def ring(arcseq):
    coords = []
    for i in arcseq:
        c = arc_coords(i)
        if coords:
            coords.extend(c[1:])
        else:
            coords.extend(c)
    return coords

def geom_to_shape(g):
    t = g["type"]
    if t == "Polygon":
        return Polygon(ring(g["arcs"][0]), [ring(h) for h in g["arcs"][1:]])
    if t == "MultiPolygon":
        polys = []
        for poly in g["arcs"]:
            polys.append(Polygon(ring(poly[0]), [ring(h) for h in poly[1:]]))
        return MultiPolygon(polys)
    raise ValueError(t)

feats = []
for g in obj["geometries"]:
    try:
        sh = geom_to_shape(g)
        if not sh.is_valid:
            sh = sh.buffer(0)
        feats.append({"name": g["properties"].get("name", ""),
                      "prov": g["properties"].get("parentName", ""),
                      "geom": sh})
    except Exception as e:
        pass

TARGETS = ["Kalomba", "Luambo", "Luiza", "Masuika", "Tshibala", "Yangala"]
def short(n):
    return n.replace("kr ", "").replace(" Zone de Santé", "").strip()

six = []
for f in feats:
    sn = short(f["name"])
    if sn in TARGETS:
        six.append(f)
six_geom = unary_union([f["geom"] for f in six])
minx, miny, maxx, maxy = six_geom.bounds
w = maxx - minx; h = maxy - miny
pad = 0.30
vminx, vmaxx = minx - w * pad, maxx + w * pad
vminy, vmaxy = miny - h * pad * 1.3, maxy + h * pad
view = box(vminx, vminy, vmaxx, vmaxy)

# neighbors (other RDC ZS visible in view)
neigh = []
for f in feats:
    if short(f["name"]) in TARGETS:
        continue
    if f["geom"].intersects(view):
        neigh.append(f)

# national outline -> international border within view (south = Angola)
print("union of all ZS...")
national = unary_union([f["geom"] for f in feats])
natl_boundary = national.boundary
intl_border = natl_boundary.intersection(view)

# Angola = part of view south/outside of RDC national area
angola = view.difference(national)

# province (Kasaï Central) outline
kc = unary_union([f["geom"] for f in feats if f["prov"] == six[0]["prov"]])

# ---- plotting ----
latm = math.radians((vminy + vmaxy) / 2)
km_per_deg_lon = 111.32 * math.cos(latm)

fig, ax = plt.subplots(figsize=(8.6, 7.4), dpi=200)
ax.set_facecolor("#eef3f7")

# Angola fill
def plot_poly(geom, **kw):
    if geom.is_empty: return
    gs = geom.geoms if geom.geom_type.startswith("Multi") else [geom]
    for p in gs:
        if p.geom_type != "Polygon": continue
        xs, ys = p.exterior.xy
        ax.fill(xs, ys, **kw)

plot_poly(angola.intersection(view), color="#f3e2bf", zorder=1)

# neighbor ZS (context)
for f in neigh:
    plot_poly(f["geom"].intersection(view), facecolor="#dfe6ec", edgecolor="#b7c2cc",
              linewidth=0.4, zorder=2)

# 6 ZS distinct palette
palette = {
    "Tshibala": "#a6cee3", "Luiza": "#cfe8f3", "Yangala": "#74b3d6",
    "Masuika": "#9ecae1", "Kalomba": "#7fb0d4", "Luambo": "#b5d8ea",
}
for f in six:
    sn = short(f["name"])
    plot_poly(f["geom"], facecolor=palette.get(sn, "#9ecae1"),
              edgecolor="#1b5e84", linewidth=1.4, zorder=4)

# province boundary (dashed)
def plot_line(geom, **kw):
    if geom.is_empty: return
    gs = geom.geoms if geom.geom_type.startswith("Multi") else [geom]
    for ln in gs:
        try:
            xs, ys = ln.xy
            ax.plot(xs, ys, **kw)
        except Exception:
            pass
plot_line(kc.boundary.intersection(view), color="#2e7d32", linewidth=1.0,
          linestyle=(0, (6, 4)), zorder=5, alpha=0.8)

# international border RDC - Angola (bold red)
plot_line(intl_border, color="#d32f2f", linewidth=3.0, zorder=7,
          solid_capstyle="round")

# labels for 6 ZS
for f in six:
    sn = short(f["name"])
    c = f["geom"].representative_point()
    ax.text(c.x, c.y, sn.upper(), ha="center", va="center", fontsize=9.5,
            fontweight="bold", color="#0b3a6f", zorder=9,
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="#0b3a6f", lw=0.7, alpha=0.9))

# chef-lieu Luiza
luiza = [f for f in six if short(f["name"]) == "Luiza"][0]
lc = luiza["geom"].representative_point()
ax.plot(lc.x, lc.y - (vmaxy - vminy) * 0.045, marker="*", markersize=15,
        color="#d32f2f", markeredgecolor="#7a0000", zorder=10)

# ANGOLA label
ax.text((vminx + vmaxx) / 2, vminy + (vmaxy - vminy) * 0.05, "ANGOLA",
        ha="center", va="center", fontsize=17, fontweight="bold",
        color="#a07c2c", zorder=8, alpha=0.85)

# DPS context labels
ax.text(vminx + w * 0.02, vmaxy - h * 0.04, "PROVINCE DU KASAÏ CENTRAL",
        fontsize=7.5, color="#2e7d32", style="italic", zorder=8)

ax.set_xlim(vminx, vmaxx); ax.set_ylim(vminy, vmaxy)
ax.set_aspect(1 / math.cos(latm))

# graticule
ax.grid(True, color="#9fb3c2", linewidth=0.4, alpha=0.6)
ax.tick_params(labelsize=7, colors="#555")
for s in ax.spines.values():
    s.set_edgecolor("#0b3a6f"); s.set_linewidth(1.4)

# north arrow
nx, ny = vmaxx - w * 0.07, vmaxy - h * 0.16
ax.annotate("N", xy=(nx, ny + h * 0.10), xytext=(nx, ny),
            ha="center", va="center", fontsize=12, fontweight="bold", color="#0b3a6f",
            arrowprops=dict(arrowstyle="-|>", color="#0b3a6f", lw=2.2), zorder=11)

# scale bar
bar_km = 25
bar_deg = bar_km / km_per_deg_lon
bx = vminx + w * 0.06
by = vminy + h * 0.07
ax.add_patch(Rectangle((bx, by), bar_deg, h * 0.012, facecolor="#0b3a6f",
                       edgecolor="#0b3a6f", zorder=11))
ax.add_patch(Rectangle((bx + bar_deg, by), bar_deg, h * 0.012, facecolor="white",
                       edgecolor="#0b3a6f", zorder=11))
ax.text(bx, by + h * 0.022, "0", fontsize=6.5, ha="center", zorder=11)
ax.text(bx + bar_deg, by + h * 0.022, f"{bar_km}", fontsize=6.5, ha="center", zorder=11)
ax.text(bx + 2 * bar_deg, by + h * 0.022, f"{2*bar_km} km", fontsize=6.5, ha="center", zorder=11)

# legend
leg = [
    Patch(facecolor="#9ecae1", edgecolor="#1b5e84", label="6 ZS · Antenne PEV Luiza"),
    Patch(facecolor="#dfe6ec", edgecolor="#b7c2cc", label="ZS voisines (RDC)"),
    Patch(facecolor="#f3e2bf", edgecolor="#d6c08a", label="Angola"),
    Line2D([0], [0], color="#d32f2f", lw=3, label="Frontière RDC – Angola"),
    Line2D([0], [0], color="#2e7d32", lw=1.2, ls="--", label="Limite provinciale"),
    Line2D([0], [0], marker="*", color="w", markerfacecolor="#d32f2f",
           markersize=12, label="Chef-lieu (Luiza)"),
]
ax.legend(handles=leg, loc="lower right", fontsize=7, framealpha=0.95,
          edgecolor="#0b3a6f", title="Légende", title_fontsize=7.5)

ax.set_title("Antenne PEV de Luiza — 6 Zones de Santé et frontière avec l'Angola",
             fontsize=11.5, fontweight="bold", color="#0b3a6f", pad=10)
fig.text(0.5, 0.012,
         "Source : OU map RDC — topojson officiel des Zones de Santé · Projection Mercator (WGS84)",
         fontsize=6.5, color="#777", ha="center")

plt.tight_layout(rect=(0, 0.03, 1, 1))
plt.savefig("/tmp/carte_luiza.png", dpi=200, bbox_inches="tight", facecolor="white")
print("saved /tmp/carte_luiza.png")
print("six bounds", six_geom.bounds)
