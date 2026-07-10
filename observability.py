"""
Phoenix(OpenTelemetry) 추적 설정.

init_tracing() 한 번 호출하면 이후 모든 OpenAI/Upstage 호출(임베딩·LLM 답변)이
자동으로 Phoenix로 전송된다. Phoenix UI: http://localhost:6006

연결: PHOENIX_ENDPOINT(기본 http://localhost:6006/v1/traces)
"""

import os

_tracer_provider = None


def init_tracing(project_name="bank-rag"):
    """추적 초기화 (중복 호출 안전). tracer_provider 반환."""
    global _tracer_provider
    if _tracer_provider is not None:
        return _tracer_provider

    from phoenix.otel import register
    from openinference.instrumentation.openai import OpenAIInstrumentor

    endpoint = os.environ.get("PHOENIX_ENDPOINT", "http://localhost:6006/v1/traces")
    _tracer_provider = register(
        project_name=project_name,
        endpoint=endpoint,
        set_global_tracer_provider=True,
    )
    # Upstage는 OpenAI 호환 SDK → OpenAI 계측기가 임베딩·챗 호출을 모두 추적
    OpenAIInstrumentor().instrument(tracer_provider=_tracer_provider)
    return _tracer_provider
