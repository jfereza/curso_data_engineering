{% set models_to_generate = codegen.get_models(prefix='base_') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}