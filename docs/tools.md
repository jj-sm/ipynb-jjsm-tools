# ipynb_jjsm_tools — photometry pipeline additions

## `plot` — image display
 
### `plot.plot_image(image_data, ...)`
Drop-in generalization of the `plot_image` you already had in the
notebook: log-stretched `imshow` by default (percentile-based `vmin`/
`vmax`, `LogNorm`), WCS-aware RA/Dec axes when you pass `wcs=...`, dual
PNG+PDF saving via `filename=` + `graphs_path=`, and support for RGB
stacks. Returns `(fig, ax)` instead of always calling `plt.show()`, so
you can keep tweaking the axes before displaying/saving.
 
```python
fig, ax = lab.plot.plot_image(
    ccd.data, wcs=ccd.wcs, title="PG 1047 (G)",
    filename="pg1047_g", graphs_path=GRAPHS_PATH,
)
```
 
### `plot.plot_image_both_styles(image_data, ...)`
Renders the same image twice — once with `text.usetex=True`, once with
mathtext only — by calling `setup.activate_tex()` in between. Handy for
checking a figure's label rendering both ways before comitting to one in
a report. Returns `{"tex": (fig, ax, status), "mathtext": (fig, ax, status)}`.
 
```python
figs = lab.plot.plot_image_both_styles(ccd.data, wcs=ccd.wcs, title="M48 (R)")
```
 
## `photometry` — aperture photometry & zero-point calibration
 
### `photometry.make_apertures(x, y, r, r_in, r_out)`
Builds the `(CircularAperture, CircularAnnulus)` pair for a set of star
positions — same as the notebook's `fotometria()`.
 
### `photometry.aperture_flux(data, apertures, sigma=3.0, min_annulus_px=5)`
Background-subtracted aperture flux (equivalent to `flujo`/`flujo_clean`
combined): estimates local sky as the sigma-clipped median of each
star's annulus, scales by aperture area, subtracts. Annuli with too few
finite pixels fall back to a background of 0 instead of poisoning the
whole array with `NaN`. Works with a bare `ndarray` or a `CCDData`
(units propagate automatically in the latter case).
 
### `photometry.instrumental_mag(flux)`
`-2.5 log10(flux)`, `NaN` for non-positive flux.
 
### `photometry.compute_zeropoint(cat_mag, inst_mag, sigma=2.0)`
Sigma-clipped zero point `ZP = cat_mag - inst_mag` over a set of matched
calibration stars. Returns `{"zp", "sigma", "n", "values"}`.
 
```python
res = lab.photometry.compute_zeropoint(g_cat_matched, m_inst_g_matched)
ZP_G, sig_ZP_G = res["zp"], res["sigma"]
```
 
### `photometry.plot_zeropoint_fit(inst_mag, cat_mag, zp, sigma, band="g", ...)`
The `m_cat = m_inst + ZP` fit plot from the notebook (inverted axes,
`AutoMinorLocator`, dashed fit line, dual PNG+PDF save).
 
### `photometry.scan_aperture_radii(radii_pix, evaluate_fn, bands)`
Generalizes the notebook's radius-optimization loop
(`evaluate_radius_per_band` + the `results_bands`/`best_radii` bookkeeping).
You supply `evaluate_fn(radius_pix) -> {"std": {band: sigma, ...}} | None`
— i.e. whatever your source-detection + matching + `compute_zeropoint`
pipeline looks like for a given task — and this sweeps the grid, collects
the scatter curve per band, and returns the radius that minimizes it.
 
```python
def evaluate(r):
    # detect sources, match to catalog, run aperture_flux + compute_zeropoint
    # per band at aperture radius r ...
    return {"std": {"g": std_g, "r": std_r, "i": std_i}}
 
results, best_radii = lab.photometry.scan_aperture_radii(
    np.arange(3.0, 10.5, 0.5), evaluate, bands=("g", "r", "i"),
)
```
 
### `photometry.plot_radius_optimization(results, best_radii, ...)`
Plots the σ-vs-radius curves from `scan_aperture_radii`, one line per
band, with a dashed vertical marker at each band's optimum.
 
## `plot` — CMD / HR diagrams
 
### `plot.magnitude_diagram(color, mag, color_label, mag_label, ...)`
Generic color-magnitude scatter: y-axis inverted by default (brighter on
top), minor tick locators, optional `ax=` to plot into an existing axes,
optional `filename=`/`graphs_path=` to save. This one function covers
every CMD panel in the notebook (`R` vs `G-R`, `R` vs `R-I`, `R` vs
`G-I`) — call it once per color index instead of copy-pasting the plot
block.
 
```python
lab.plot.magnitude_diagram(
    catalogo_m48["g_r"].to_numpy(), catalogo_m48["mag_r"].to_numpy(),
    color_label=r"$G-R$ [mag]", mag_label=r"$R$ [mag]",
    title=r"M48: $R$ vs $(G-R)$",
    filename="cmd_m48_gr", graphs_path=GRAPHS_PATH,
)
```
 
### `plot.color_magnitude_diagram(...)` / `plot.hr_diagram(...)`
Thin aliases of `magnitude_diagram` for readability at the call site —
`hr_diagram` just defaults its axis labels to `B-V` / `M_V`. Pass
`invert_color=True` if you're plotting against temperature directly
(hot stars on the left).
 
```python
lab.plot.hr_diagram(catalogo["g_r"], catalogo["abs_mag_g"], title="M48 HR diagram")
```
 
### Isochrones on a CMD / HR diagram
`magnitude_diagram` (and therefore both aliases) now accepts
`isochrone=(iso_color, iso_mag)` — two already-shifted arrays it just
overlays as a line with its own label/color. Use `shift_isochrone` to
do the distance-modulus/extinction/reddening bookkeeping you had
scattered across cells 102–107, once, generically:
 
```python
iso_c, iso_r = lab.plot.shift_isochrone(
    iso_table["gmag"] - iso_table["rmag"], iso_table["rmag"],
    distance_modulus=mod_distancia, extinction=A_r, reddening=E_gr,
    mag_range=(r_min, r_max),   # optional: trims the line to your plot's range
)
 
lab.plot.magnitude_diagram(
    catalogo_m48["g_r"].to_numpy(), catalogo_m48["mag_r"].to_numpy(),
    color_label=r"$G-R$ [mag]", mag_label=r"$R$ [mag]",
    isochrone=(iso_c, iso_r), isochrone_label="Isócrona 450 Myr",
    filename="cmd_m48_gr_iso", graphs_path=GRAPHS_PATH,
)
```
 
## `plot` — PSF profile inspection
 
### `plot.plot_star_profile(img, cx, cy, ...)`
The notebook's `plot_star_profile_joint`, generalized to take its
save-path/DPI/flag as arguments instead of reading the globals
`SAVE_PLOTS`/`GRAPHS_PATH`/`DPI`. Crops a window around `(cx, cy)` and
draws the cutout plus its X and Y pixel profiles, each with a 1D
Gaussian fit (`amp, cen, sigma, offset`) — read off `sigma` (in pixels)
to sanity-check your aperture radius or FWHM assumption. Falls back to
unfit profiles (still plotted, just no Gaussian overlay) if `scipy`
isn't installed or a fit doesn't converge.
 
```python
fig, fit_results = lab.plot.plot_star_profile(
    COMMON["r"].data, x_zoom, y_zoom, label="M48 Stack R — Estrella 10",
    window=20, filename="profile_star10_r", graphs_path=GRAPHS_PATH,
)
sigma_x = fit_results["x"]["popt"][2]
sigma_y = fit_results["y"]["popt"][2]
```
 
`plot.gaussian_1d(x, amp, cen, sigma, offset)` is also exported
directly if you want to reuse the model function elsewhere (e.g. to
build a `true_popt` reference curve for a simulated star).
 
## `plot` — generic scatter + fit + residuals
 
### `plot.scatter_with_fit(x, y, fit=None, ...)`
The two-axes "data on top, residuals below, shared x-axis" layout from
your Sérsic-profile plot, generalized so it isn't tied to Sérsic
specifically. `fit` can be:
- a **callable** `f(x) -> y` — plotted on a `fit_x` grid (finer than
  the data if you like) and evaluated at `x` for the residuals, or
- a **precomputed array** already aligned one-to-one with `x`.
Leave `fit=None` for a plain scatter (single axes, no residual panel).
 
```python
fig, (ax, ax_r) = lab.plot.scatter_with_fit(
    df_p1_filtered["a"]**(1/4), df_p1_filtered["mu_V"],
    fit=lambda a4: sersic_n4(a4**4, mu_e, r_e),
    fit_x=np.linspace(0, df_p1_filtered["a"].max()**(1/4), 200),
    xlabel=r"$r^{1/4}$ [arcsec$^{1/4}$]", ylabel=r"$\mu_V$ [mag arcsec$^{-2}$]",
    data_label=r"$r \ge 2''$ (Datos Seleccionados)", fit_label="Ajuste Sérsic (n=4)",
    residual_ylim=(-0.9, 0.9), invert_y=True,
    filename="plot_2_residual_n4", graphs_path=GRAPHS_PATH, dpi=DPI,
)
```
 
Note the fit passed above takes `r^{1/4}` and internally un-does the
`**4` to call `sersic_n4` on `r` directly — write whatever wrapper
matches your model's natural input; `scatter_with_fit` only cares that
it accepts the same x you're plotting.
 