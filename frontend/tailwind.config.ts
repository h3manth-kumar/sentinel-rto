import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          DEFAULT: '#0B5CFF',
          primary: '#0B5CFF',
        },
        risk: {
          safe: '#10B981',
          warning: '#F59E0B',
          critical: '#EF4444',
        },
        surface: {
          dark: '#0F172A',
          card: '#1E293B',
          white: '#FFFFFF',
          offwhite: '#FAFAFA'
        }
      },
      fontFamily: {
        sans: ['var(--font-inter)'],
        mono: ['var(--font-jetbrains-mono)'],
      }
    },
  },
  plugins: [],
}
export default config
