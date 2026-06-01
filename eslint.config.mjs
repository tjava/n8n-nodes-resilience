import eslint from '@eslint/js';
import { n8nCommunityNodesPlugin } from '@n8n/eslint-plugin-community-nodes';
import { globalIgnores } from 'eslint/config';
import { createTypeScriptImportResolver } from 'eslint-import-resolver-typescript';
import importPlugin from 'eslint-plugin-import-x';
import n8nNodesBasePlugin from 'eslint-plugin-n8n-nodes-base';
import tseslint from 'typescript-eslint';

export default tseslint.config(
	globalIgnores(['dist']),
	{
		files: ['**/*.ts'],
		extends: [
			eslint.configs.recommended,
			tseslint.configs.recommended,
			n8nCommunityNodesPlugin.configs.recommended,
			importPlugin.configs['flat/recommended'],
		],
		rules: {
			'prefer-spread': 'off',
			'no-console': 'error',
		},
	},
	{
		plugins: { 'n8n-nodes-base': n8nNodesBasePlugin },
		settings: {
			'import-x/resolver-next': [createTypeScriptImportResolver()],
		},
	},
	{
		files: ['package.json'],
		rules: {
			...n8nNodesBasePlugin.configs.community.rules,
		},
		languageOptions: {
			parser: tseslint.parser,
			parserOptions: {
				extraFileExtensions: ['.json'],
			},
		},
	},
	{
		files: ['./nodes/**/*.ts'],
		rules: {
			...n8nNodesBasePlugin.configs.nodes.rules,
			'n8n-nodes-base/node-class-description-inputs-wrong-regular-node': 'off',
			'n8n-nodes-base/node-class-description-outputs-wrong': 'off',
			'n8n-nodes-base/node-param-type-options-max-value-present': 'off',
		},
	},
);
