

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
