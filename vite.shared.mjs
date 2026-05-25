/** @typedef {import('vite-plus/fmt').OxfmtConfig} OxfmtConfig */
/** @typedef {import('vite-plus/lint').OxlintConfig} OxlintConfig */

/** @type {OxfmtConfig} */
export const sharedFmtConfig = {
  ignorePatterns: [
    'js-binding*.*',
    'npm/**',
    '**/dist/**',
    '**/target/**',
    '**/node_modules/**',
    '**/coverage/**',
    '**/report/**',
  ],
  printWidth: 120,
  semi: false,
  singleQuote: true,
  sortPackageJson: true,
}

/** @type {OxlintConfig} */
export const sharedLintConfig = {
  categories: {
    correctness: 'error',
    suspicious: 'error',
    perf: 'error',
    style: 'error',
    restriction: 'error',
  },
  globals: {
    __dirname: 'off',
    __filename: 'off',
    clearImmediate: 'readonly',
    exports: 'off',
    global: 'readonly',
    module: 'off',
    require: 'off',
    setImmediate: 'readonly',
  },
  ignorePatterns: [
    'dist/',
    'coverage/',
    'report/',
    '**/*.mjs',
    '**/dist/**',
    '**/target/**',
    '**/node_modules/**',
    '**/coverage/**',
    '**/report/**',
  ],
  options: {
    typeAware: true,
    typeCheck: true,
  },
  rules: {
    'explicit-module-boundary-types': 'off',
    'unicorn/no-null': 'off',
    'explicit-function-return-type': 'off',
    'no-rest-spread-properties': 'off',
    'no-optional-chaining': 'off',
    'no-async-await': 'off',
    'no-plusplus': 'off',
    'no-undefined': 'off',
    'no-magic-numbers': 'off',
    yoda: 'off',
    'new-cap': 'off',
    'sort-keys': 'off',
    'consistent-type-imports': 'error',
    'no-ternary': 'off',
    'init-declarations': 'off',
    'func-names': 'off',
    'consistent-type-definitions': 'off',
    'sort-imports': 'off',
    'consistent-indexed-object-style': 'off',
    'func-style': 'off',
    'prefer-exponentiation-operator': 'off',
    'max-params': [
      'error',
      {
        max: 6,
      },
    ],
    'max-statements': [
      'error',
      {
        max: 40,
      },
    ],
    'unicorn/filename-case': [
      'error',
      {
        case: 'kebabCase',
      },
    ],
    'no-unused-vars': [
      'error',
      {
        args: 'all',
        argsIgnorePattern: '^_',
        caughtErrors: 'all',
        caughtErrorsIgnorePattern: '^_',
        destructuredArrayIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        ignoreRestSiblings: true,
      },
    ],
    '@typescript-eslint/ban-ts-comment': [
      'error',
      {
        minimumDescriptionLength: 10,
      },
    ],
  },
}

/** @type {NonNullable<OxlintConfig['rules']>} */
export const sharedTestLintRules = {
  'no-process-exit': 'allow',
  'id-length': 'allow',
  '@typescript-eslint/no-unsafe-type-assertion': 'allow',
  'no-undefined': 'off',
  'no-async-await': 'off',
  'no-console': 'allow',
  'no-continue': 'allow',
  'no-new': 'allow',
  curly: 'allow',
  'prefer-destructuring': 'allow',
  'prefer-template': 'allow',
  'arrow-body-style': 'allow',
  'eslint/no-await-in-loop': 'allow',
  'unicorn/no-await-expression-member': 'allow',
  'unicorn/consistent-function-scoping': 'allow',
  'unicorn/numeric-separators-style': 'allow',
  'unicorn/prefer-optional-catch-binding': 'allow',
  'unicorn/catch-error-name': 'allow',
  'unicorn/no-array-for-each': 'allow',
  'unicorn/no-array-reduce': 'allow',
  'unicorn/prefer-spread': 'allow',
  'unicorn/prefer-array-index-of': 'allow',
  'unicorn/no-console-spaces': 'allow',
  'no-empty-function': 'allow',
  'no-async-promise-executor': 'allow',
  'max-statements': 'allow',
}
