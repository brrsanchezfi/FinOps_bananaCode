

class TestSkusNoFacturables:
    """El consumo no facturable no puede tumbar el chequeo de precios.

    `price_match` es de severidad error: en qa y prd, con
    `fail_pipeline_on_error: true`, un SKU sin precio de lista por DISENO
    tumbaba el pipeline. `GENIE_FREE_USAGE` aparecio en el primer despliegue
    real con 53 registros, 74.91 DBUs y costo 0.
    """

    def test_el_repositorio_ignora_el_consumo_gratuito(self, conf_dir):
        from finops.config import load_config

        cfg = load_config("dev", conf_dir=conf_dir, use_env_vars=False, use_local_overlay=False)
        patrones = cfg.get("quality.checks.price_match_ignore_skus") or []
        assert "*_FREE_USAGE" in patrones, (
            "conf/base.yml debe ignorar el consumo no facturable de Databricks"
        )

    def test_el_patron_cubre_los_skus_gratuitos_y_no_los_demas(self):
        from finops.quality.checks import sku_is_ignored

        patrones = ["*_FREE_USAGE"]
        assert sku_is_ignored("GENIE_FREE_USAGE", patrones)
        assert sku_is_ignored("genie_free_usage", patrones)
        # Un SKU facturable no puede caer aqui: seria gasto que deja de vigilarse.
        assert not sku_is_ignored("PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST_3", patrones)
        assert not sku_is_ignored("STANDARD_ALL_PURPOSE_COMPUTE", patrones)


class TestRetractacionesDeFacturacion:
    """Una retractacion de Databricks no es un costo negativo defectuoso.

    Cuando se corrige consumo ya facturado, la fuente emite una fila
    `RETRACTION` con cantidad NEGATIVA mas una `RESTATEMENT` con el valor
    corregido; la negativa anula a la original al sumar. Contarla contra
    `max_negative_cost_rows` (0 por defecto, severidad error) tumbaba el
    pipeline en qa y prd por una correccion legitima.

    Observado en una cuenta real: 4 filas, con `usage_quantity` -0.5 en las
    retractaciones.
    """

    def test_una_retractacion_no_hace_fallar_el_chequeo(self):
        from finops.quality.checks import evaluate_negative_cost

        resultado = evaluate_negative_cost(0, 0, "silver.slv_usage_priced", retraction_rows=2)
        assert resultado.passed
        assert "2 retractacion" in resultado.message, (
            "la exclusion debe quedar visible en el mensaje, no ser silenciosa"
        )

    def test_un_costo_negativo_sin_explicacion_sigue_fallando(self):
        """El chequeo existe para atrapar defectos de valorizacion: eso no cambia."""
        from finops.quality.checks import evaluate_negative_cost

        resultado = evaluate_negative_cost(3, 0, "silver.slv_usage_priced", retraction_rows=2)
        assert not resultado.passed
        assert resultado.severity == "error"

    def test_sin_retractaciones_el_mensaje_no_cambia(self):
        from finops.quality.checks import evaluate_negative_cost

        resultado = evaluate_negative_cost(0, 0, "silver.slv_usage_priced")
        assert resultado.passed
        assert "retractacion" not in resultado.message
