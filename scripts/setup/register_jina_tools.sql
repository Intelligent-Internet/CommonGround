-- Register Jina tools (OpenAPI service -> s.jina.ai / r.jina.ai)
-- Docs:
--   https://s.jina.ai/docs
--   https://r.jina.ai/docs

INSERT INTO resource.tools (
    project_id,
    tool_name,
    target_subject,
    parameters,
    description,
    after_execution,
    options
) VALUES (
    'proj_mvp_001',
    'jina_search',
    'cg.v1r3.proj_mvp_001.public.cmd.tool.jina.search',
    '{
        "type": "object",
        "properties": {
            "query": { "type": "string", "description": "Search query" },
            "count": { "type": "integer", "default": 5 }
        },
        "required": ["query"]
    }',
    'Search web content through Jina Search API.',
    'suspend',
    '{
        "jina": {
            "mode": "search",
            "base_url": "https://s.jina.ai",
            "path": "/search",
            "method": "GET",
            "auth_env": "JINA_API_KEY"
        }
    }'
)
ON CONFLICT (project_id, tool_name)
DO UPDATE SET
    target_subject = EXCLUDED.target_subject,
    parameters = EXCLUDED.parameters,
    description = EXCLUDED.description,
    after_execution = EXCLUDED.after_execution,
    options = EXCLUDED.options;

INSERT INTO resource.tools (
    project_id,
    tool_name,
    target_subject,
    parameters,
    description,
    after_execution,
    options
) VALUES (
    'proj_mvp_001',
    'jina_reader',
    'cg.v1r3.proj_mvp_001.public.cmd.tool.jina.read',
    '{
        "type": "object",
        "properties": {
            "url": { "type": "string", "description": "URL to read" },
            "respondWith": { "type": "string", "description": "content|markdown|html" }
        },
        "required": ["url"]
    }',
    'Read and normalize page content through Jina Reader API.',
    'suspend',
    '{
        "jina": {
            "mode": "reader",
            "base_url": "https://r.jina.ai",
            "endpoint_template": "/{url}",
            "method": "GET",
            "auth_env": "JINA_API_KEY"
        }
    }'
)
ON CONFLICT (project_id, tool_name)
DO UPDATE SET
    target_subject = EXCLUDED.target_subject,
    parameters = EXCLUDED.parameters,
    description = EXCLUDED.description,
    after_execution = EXCLUDED.after_execution,
    options = EXCLUDED.options;
