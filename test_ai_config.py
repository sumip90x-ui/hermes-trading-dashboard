import importlib.util
from pathlib import Path


def load_app_module():
    app_path = Path(__file__).with_name('app.py')
    spec = importlib.util.spec_from_file_location('dashboard_app_under_test', app_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_ai_config_creates_safe_defaults(tmp_path, monkeypatch):
    module = load_app_module()
    cfg_path = tmp_path / 'ai_config.json'
    monkeypatch.setattr(module, 'AI_CONFIG_FILE', cfg_path)

    cfg = module.load_ai_config()

    assert cfg['simple_provider'] == 'openrouter'
    assert cfg['simple_model'] == 'openai/gpt-4.1-mini'
    assert cfg['quality_provider'] == 'anthropic'
    assert cfg['quality_model'] == 'claude-sonnet-4-5'
    assert cfg['trade_provider'] == 'anthropic'
    assert cfg['trade_model'] == 'claude-sonnet-4-5'
    assert cfg_path.exists()


def test_save_ai_config_only_accepts_allowed_simple_values(tmp_path, monkeypatch):
    module = load_app_module()
    cfg_path = tmp_path / 'ai_config.json'
    monkeypatch.setattr(module, 'AI_CONFIG_FILE', cfg_path)

    cfg = module.save_ai_config({'simple_provider': 'openrouter', 'simple_model': 'openai/gpt-5-mini'})
    assert cfg['simple_provider'] == 'openrouter'
    assert cfg['simple_model'] == 'openai/gpt-5-mini'
    assert cfg['trade_provider'] == 'anthropic'
    assert cfg['trade_model'] == 'claude-sonnet-4-5'


def test_save_ai_config_rejects_trade_changes_and_free_models(tmp_path, monkeypatch):
    module = load_app_module()
    cfg_path = tmp_path / 'ai_config.json'
    monkeypatch.setattr(module, 'AI_CONFIG_FILE', cfg_path)

    try:
        module.save_ai_config({
            'simple_provider': 'openrouter',
            'simple_model': 'mistralai/free-model',
            'trade_provider': 'openrouter',
            'trade_model': 'openai/gpt-4o-mini',
        })
    except ValueError as exc:
        assert 'not allowed' in str(exc)
    else:
        raise AssertionError('expected ValueError for non-allowed/free simple model')

    cfg = module.load_ai_config()
    assert cfg['trade_provider'] == 'anthropic'
    assert cfg['trade_model'] == 'claude-sonnet-4-5'


def test_ai_config_routes_do_not_allow_trade_mutation(tmp_path, monkeypatch):
    module = load_app_module()
    cfg_path = tmp_path / 'ai_config.json'
    monkeypatch.setattr(module, 'AI_CONFIG_FILE', cfg_path)
    client = module.app.test_client()

    get_resp = client.get('/api/ai/config')
    assert get_resp.status_code == 200
    body = get_resp.get_json()
    assert body['config']['trade_provider'] == 'anthropic'
    assert body['config']['trade_model'] == 'claude-sonnet-4-5'
    assert any(opt['provider'] == 'openrouter' and opt['model'] == 'openai/gpt-4.1-mini' for opt in body['options'])

    post_resp = client.post('/api/ai/config', json={
        'simple_provider': 'openrouter',
        'simple_model': 'openai/gpt-4o-mini',
        'trade_provider': 'openrouter',
        'trade_model': 'openai/gpt-4o-mini',
    })
    assert post_resp.status_code == 200
    cfg = post_resp.get_json()['config']
    assert cfg['simple_model'] == 'openai/gpt-4o-mini'
    assert cfg['trade_provider'] == 'anthropic'
    assert cfg['trade_model'] == 'claude-sonnet-4-5'
