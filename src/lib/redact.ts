/**
 * Removal of secrets from anything that can end up in a log, a state or a sendTo answer.
 */

const PATTERNS: RegExp[] = [
    /(authorization\s*[:=]\s*)(bearer|token|basic)?\s*[^\s,;"'}]+/gi,
    /\b(bearer|token)\s+[A-Za-z0-9._~+/=-]{8,}/gi,
    /apiv3_[A-Za-z0-9_-]+/g,
    /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g, // JWT
    /([?&](?:token|auth|password|p)=)[^&\s]+/gi,
];

const extraSecrets = new Set<string>();

/**
 * Registers a concrete secret (e.g. the configured token) that must be masked everywhere.
 *
 * @param secret the secret value
 */
export function registerSecret(secret: string | undefined | null): void {
    if (secret && secret.length >= 4) {
        extraSecrets.add(secret);
    }
}

/** Removes all registered secrets (used in tests). */
export function clearSecrets(): void {
    extraSecrets.clear();
}

/**
 * Returns `input` with all secrets replaced by `***`.
 *
 * @param input arbitrary text or value
 */
export function redact(input: unknown): string {
    let text: string;
    if (input instanceof Error) {
        text = input.message;
    } else if (typeof input === 'string') {
        text = input;
    } else {
        try {
            text = JSON.stringify(input) ?? String(input);
        } catch {
            text = String(input);
        }
    }
    for (const secret of extraSecrets) {
        text = text.split(secret).join('***');
    }
    for (const re of PATTERNS) {
        text = text.replace(re, (match: string, prefix?: string) =>
            typeof prefix === 'string' && match.startsWith(prefix) ? `${prefix}***` : '***',
        );
    }
    return text;
}
