/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /*
   This is a slight modification of the existing getinfo.js.
   I've added the random key to decrease the chances of a PDF
   containing markup causing problems with the post-processing.
   Also, I've set the verbosity to INFOS and a few other custom
   parameter settings in the call to load the PDF.
 */
//
// Basic node example that prints document metadata and text content.
// Requires single file built version of PDF.js -- please run
// `gulp singlefile` before running the example.
//

// Run `gulp dist-install` to generate 'pdfjs-dist' npm package files.
const pdfjsLib = require("pdfjs-dist/legacy/build/pdf.js");


// Loading file from file system into typed array
const pdfPath =
  process.argv[2] || "../../web/compressed.tracemonkey-pldi-09.pdf";
const randKey = Math.floor(Math.random() * 1000000000);
console.log("# Random Key: " + randKey);
// Will be using promises to load document, pages and misc data instead of
// callback.

const loadingTask = pdfjsLib.getDocument({
    "url": pdfPath,
    "verbosity": pdfjsLib.VerbosityLevel.INFOS,
    "ignoreErrors": false,
    "useSystemFonts": true,
    "stopAtErrors": false
    });

loadingTask.promise
  .then(function (doc) {
    const numPages = doc.numPages;
    console.log("# Document Loaded key=" + randKey);
    console.log("# Number of Pages: " + numPages + " key=" + randKey);
    console.log();

    let lastPromise; // will be used to chain promises
    lastPromise = doc.getMetadata().then(function (data) {
      console.log("#  Metadata Is Loaded key=" + randKey);
      console.log("## Info key=" + randKey);
      console.log(JSON.stringify(data.info, null, 2));
      console.log();
      if (data.metadata) {
        console.log("## Metadata key=" + randKey);
        console.log(JSON.stringify(data.metadata.getAll(), null, 2));
        console.log();
      }
    });

    const loadPage = function (pageNum) {
      return doc.getPage(pageNum).then(function (page) {
        console.log("# Page " + pageNum + " key=" + randKey);
        const viewport = page.getViewport({ scale: 1.0 });
        console.log("# Size: " + viewport.width + "x" + viewport.height + " key=" + randKey);
        console.log();
        return page
          .getTextContent()
          .then(function (content) {
            // Content contains lots of information about the text layout and
            // styles, but we need only strings at the moment
            const strings = content.items.map(function (item) {
              return item.str;
            });
            console.log("## Text Content key=" + randKey);
            console.log(strings.join(" "));
          })
          .then(function () {
            console.log();
          });
      });
    };
    // Loading of the first page will wait on metadata and subsequent loadings
    // will wait on the previous pages.
    for (let i = 1; i <= numPages; i++) {
      lastPromise = lastPromise.then(loadPage.bind(null, i));
    }
    return lastPromise;
  })
  .then(
    function () {
      console.log("# End of Document key=" + randKey);
    },
    function (err) {
      console.error("# Error key=" + randKey +": "+ err);
    }
  );