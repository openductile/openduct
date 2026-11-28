// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Well-known model services, so an AI credential is a choice and a key: picking one fills in its address, its API's
// shape and how it takes its key, each of which can still be changed. The engine names none of them - a credential
// is an address, a key and a shape - and any other service that speaks the standard works too.

export type Provider = {
  id: string;
  name: string;
  /** Its logo, when it has one (see flow/brands.ts). */
  brand?: string;
  base_url: string;
  api: "chat_completions" | "messages";
  key_header?: string;
  headers?: string;
  /** It runs on this machine and takes no key. */
  keyless?: boolean;
  /** What of its address is the person's own to fill in (a resource's name, a region). */
  edit?: string;
};

export const PROVIDERS: Provider[] = [
  { id: "openai", name: "OpenAI", base_url: "https://api.openai.com/v1", api: "chat_completions" },
  {
    id: "anthropic",
    name: "Anthropic",
    brand: "anthropic",
    base_url: "https://api.anthropic.com/v1",
    api: "messages",
    key_header: "x-api-key",
    headers: '{"anthropic-version": "2023-06-01"}',
  },
  { id: "gemini", name: "Google Gemini", brand: "googlegemini", base_url: "https://generativelanguage.googleapis.com/v1beta/openai", api: "chat_completions" },
  { id: "mistral", name: "Mistral AI", brand: "mistralai", base_url: "https://api.mistral.ai/v1", api: "chat_completions" },
  { id: "groq", name: "Groq", base_url: "https://api.groq.com/openai/v1", api: "chat_completions" },
  { id: "openrouter", name: "OpenRouter", brand: "openrouter", base_url: "https://openrouter.ai/api/v1", api: "chat_completions" },
  { id: "together", name: "Together AI", base_url: "https://api.together.xyz/v1", api: "chat_completions" },
  // G7 more AI steps: the most used services a template names that were not here yet.
  {
    id: "azure",
    name: "Azure OpenAI",
    base_url: "https://YOUR-RESOURCE.openai.azure.com/openai/v1",
    api: "chat_completions",
    key_header: "api-key",
    edit: "put your resource's name in the address; a step's model is your deployment's name",
  },
  {
    id: "bedrock",
    name: "Amazon Bedrock",
    base_url: "https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1",
    api: "chat_completions",
    edit: "put your region in the address; the key is a Bedrock API key",
  },
  { id: "deepseek", name: "DeepSeek", base_url: "https://api.deepseek.com/v1", api: "chat_completions" },
  { id: "xai", name: "xAI", base_url: "https://api.x.ai/v1", api: "chat_completions" },
  { id: "ollama", name: "Ollama, on this machine", brand: "ollama", base_url: "http://localhost:11434/v1", api: "chat_completions", keyless: true },
  { id: "lmstudio", name: "LM Studio, on this machine", base_url: "http://localhost:1234/v1", api: "chat_completions", keyless: true },
  { id: "other", name: "Any other", base_url: "", api: "chat_completions" },
];

export function providerOf(id: string | undefined): Provider | undefined {
  return PROVIDERS.find((p) => p.id === id);
}

/** The credential's fields a provider fills in. */
export function filledBy(p: Provider): Record<string, string> {
  const out: Record<string, string> = { provider: p.id, base_url: p.base_url, api: p.api };
  if (p.key_header) out.key_header = p.key_header;
  if (p.headers) out.headers = p.headers;
  return out;
}
