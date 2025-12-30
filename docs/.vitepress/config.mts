import { defineConfig } from 'vitepress'

export default defineConfig({
    title: "aerofs",
    description: "High-Performance Asynchronous File I/O for Python",
    lang: 'en-US',
    base: '/aerofs/',
    cleanUrls: true,
    lastUpdated: true,

    head: [
        ['link', { rel: 'icon', href: '/aerofs/favicon.ico' }],
        ['link', { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap' }],
        ['link', { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap' }]
    ],

    themeConfig: {
        logo: { text: 'aerofs' },

        siteTitle: 'AeroFS',

        nav: [
            { text: 'Home', link: '/' },
            { text: 'Guide', link: '/guide/getting-started' },
            { text: 'GitHub', link: 'https://github.com/ohmyarthur/aerofs' }
        ],

        sidebar: [
            {
                text: 'Introduction',
                items: [
                    { text: 'Getting Started', link: '/guide/getting-started' },
                ]
            },
            {
                text: 'Guides',
                items: [
                    { text: 'File Operations', link: '/guide/file-operations' },
                    { text: 'OS Operations', link: '/guide/os-operations' },
                    { text: 'Standard I/O', link: '/guide/standard-io' },
                    { text: 'Temporary Files', link: '/guide/tempfile' }
                ]
            },
            {
                text: 'API Reference',
                items: [
                    { text: 'Core (open, file)', link: '/api/core' },
                    { text: 'OS (aerofs.os)', link: '/api/os' },
                    { text: 'Tempfile', link: '/api/tempfile' }
                ]
            },
            {
                text: 'Resources',
                items: [
                    { text: 'Performance Tips', link: '/guide/performance' },
                    { text: 'FAQ', link: '/guide/faq' }
                ]
            }
        ],

        socialLinks: [
            { icon: 'github', link: 'https://github.com/ohmyarthur/aerofs' }
        ],

        footer: {
            message: 'Released under the Apache 2.0 License.',
            copyright: 'Copyright © 2025 Arthur'
        },

        search: {
            provider: 'local'
        },

        outline: {
            level: [2, 3],
            label: 'On this page'
        }
    },
})
