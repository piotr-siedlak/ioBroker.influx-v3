// ioBroker eslint configuration (flat config) for the TypeScript adapter
import config from '@iobroker/eslint-config';

export default [
    ...config,
    {
        ignores: [
            '.dev-server/',
            '.vscode/',
            'build/',
            'admin/',
            'test/**/*.js',
            '*.config.mjs',
            '**/adapter-config.d.ts',
        ],
    },
    {
        // same relaxation as ioBroker.influxdb: JSDoc is required on exported/public API only by review
        rules: {
            'jsdoc/require-jsdoc': 'off',
            'jsdoc/require-param': 'off',
        },
    },
    {
        // test helpers: inferred return types and async test bodies are fine
        files: ['test/**/*.ts'],
        rules: {
            '@typescript-eslint/explicit-function-return-type': 'off',
            '@typescript-eslint/require-await': 'off',
        },
    },
    {
        languageOptions: {
            parserOptions: {
                projectService: true,
                tsconfigRootDir: import.meta.dirname,
            },
        },
    },
];
