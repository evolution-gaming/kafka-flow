/**
 * Swizzled @theme/DocItem/Content.
 *
 * Restores the Docusaurus 1 behaviour: always render the front-matter title as
 * the page's <h1> at the top of the doc, even when the markdown already starts
 * with its own heading. (The upstream component suppresses the synthetic title
 * when the content has a top-level h1, which moves the title into the
 * breadcrumb only — see the `typeof contentTitle === 'undefined'` check that is
 * intentionally dropped below.)
 *
 * Note: on docs that already begin with an `#` heading (overview, faq) this
 * produces two h1s, matching how the old D1 site rendered those pages.
 */
import React from "react";
import clsx from "clsx";
import {ThemeClassNames} from "@docusaurus/theme-common";
import {useDoc} from "@docusaurus/plugin-content-docs/client";
import Heading from "@theme/Heading";
import MDXContent from "@theme/MDXContent";

function useSyntheticTitle() {
  const {metadata, frontMatter} = useDoc();
  // Always render the title unless explicitly hidden via front matter.
  if (frontMatter.hide_title) {
    return null;
  }
  return metadata.title;
}

export default function DocItemContent({children}) {
  const syntheticTitle = useSyntheticTitle();
  return (
    <div className={clsx(ThemeClassNames.docs.docMarkdown, "markdown")}>
      {syntheticTitle && (
        <header>
          <Heading as="h1">{syntheticTitle}</Heading>
        </header>
      )}
      <MDXContent>{children}</MDXContent>
    </div>
  );
}
