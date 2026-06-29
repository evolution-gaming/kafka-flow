// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    {
      type: 'category',
      label: 'Kafka-Flow',
      collapsed: false,
      items: ['overview', 'setup', 'faq', 'styleguide', 'persistence'],
    },
  ],
};

module.exports = sidebars;
