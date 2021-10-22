/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.tika.parsers.pdf;

import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pdfbox.contentstream.PDFGraphicsStreamEngine;
import org.apache.pdfbox.contentstream.operator.MissingOperandException;
import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSObjectKey;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.filter.MissingImageReaderException;
import org.apache.pdfbox.io.IOUtils;
import org.apache.pdfbox.pdmodel.MissingResourceException;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.color.PDColor;
import org.apache.pdfbox.pdmodel.graphics.color.PDDeviceGray;
import org.apache.pdfbox.pdmodel.graphics.color.PDDeviceRGB;
import org.apache.pdfbox.pdmodel.graphics.color.PDPattern;
import org.apache.pdfbox.pdmodel.graphics.form.PDFormXObject;
import org.apache.pdfbox.pdmodel.graphics.form.PDTransparencyGroup;
import org.apache.pdfbox.pdmodel.graphics.image.PDImage;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.pdmodel.graphics.pattern.PDAbstractPattern;
import org.apache.pdfbox.pdmodel.graphics.pattern.PDTilingPattern;
import org.apache.pdfbox.pdmodel.graphics.state.PDExtendedGraphicsState;
import org.apache.pdfbox.pdmodel.graphics.state.PDSoftMask;
import org.apache.pdfbox.pdmodel.graphics.state.RenderingMode;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.apache.pdfbox.util.Matrix;
import org.apache.pdfbox.util.Vector;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.TikaMemoryLimitException;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.BoundedInputStream;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;

/**
 * Initially copied verbatim from PDFBox.  We had to use the GraphicsEngine
 * to extract actual images correctly. We can't just take the raw streams.
 *
 * We had to bolt onto this, the ability to track which streams were processed,
 * and also process, e.g., ICC profiles.
 */
class ImageGraphicsEngine extends PDFGraphicsStreamEngine {

    //We're currently copying images to byte[].  We should
    //limit the length to avoid OOM on crafted files.
    private static final long MAX_IMAGE_LENGTH_BYTES = 100 * 1024 * 1024;

    private static final List<String> JPEG =
            Arrays.asList(COSName.DCT_DECODE.getName(), COSName.DCT_DECODE_ABBREVIATION.getName());


    private static final List<String> JP2 = Collections.singletonList(COSName.JPX_DECODE.getName());

    private static final List<String> JB2 =
            Collections.singletonList(COSName.JBIG2_DECODE.getName());
    final List<IOException> exceptions = new ArrayList<>();
    private final EmbeddedDocumentExtractor embeddedDocumentExtractor;
    private final Set<COSStream> processedStreams;
    private final AtomicInteger imageCounter;
    private final Metadata parentMetadata;
    private final XHTMLContentHandler xhtml;
    private final ParseContext parseContext;
    private final ParseState parseState;
    private static boolean FASTER = false;
    //TODO: parameterize this ?
    private boolean useDirectJPEG = true;
    private COSObjectKey currObjectKey = null; // object key currently being processed

    protected ImageGraphicsEngine(PDPage page, ParseState parseState) {
        super(page);
        this.parseState = parseState;
        this.embeddedDocumentExtractor = parseState.embeddedDocumentExtractor;
        this.processedStreams = parseState.visitedStreams;
        this.imageCounter = parseState.imageCounter;
        this.xhtml = parseState.xhtml;
        this.parentMetadata = parseState.parentMetadata;
        this.parseContext = parseState.parseContext;
    }

    //nearly directly copied from PDFBox ExtractImages
    private static void writeToBuffer(PDImage pdImage, String suffix, boolean directJPEG,
                                      OutputStream out) throws IOException, TikaException {
        COSBase base = pdImage.getCOSObject();
        if (!FASTER && "jpg".equals(suffix)) {

            String colorSpaceName = pdImage.getColorSpace().getName();
            if (directJPEG || (PDDeviceGray.INSTANCE.getName().equals(colorSpaceName) ||
                    PDDeviceRGB.INSTANCE.getName().equals(colorSpaceName))) {
                // RGB or Gray colorspace: get and write the unmodified JPEG stream
                InputStream data = pdImage.createInputStream(JPEG);
                try {
                    copyUpToMaxLength(data, out);
                } finally {
                    IOUtils.closeQuietly(data);
                }
            } else {
                BufferedImage image = pdImage.getImage();
                if (image != null) {
                    // for CMYK and other "unusual" colorspaces, the JPEG will be converted
                    ImageIOUtil.writeImage(image, suffix, out);
                }
            }
        } else if ("jp2".equals(suffix)) {
            String colorSpaceName = pdImage.getColorSpace().getName();
            if (directJPEG || !hasMasks(pdImage) &&
                    (PDDeviceGray.INSTANCE.getName().equals(colorSpaceName) ||
                            PDDeviceRGB.INSTANCE.getName().equals(colorSpaceName))) {
                // RGB or Gray colorspace: get and write the unmodified JPEG2000 stream
                InputStream data = pdImage.createInputStream(JP2);
                try {
                    copyUpToMaxLength(data, out);
                } finally {
                    IOUtils.closeQuietly(data);
                }
            } else {
                // for CMYK and other "unusual" colorspaces, the image will be converted
                BufferedImage image = pdImage.getImage();
                if (image != null) {
                    // for CMYK and other "unusual" colorspaces, the JPEG will be converted
                    ImageIOUtil.writeImage(image, "jpeg2000", out);
                }
            }
        } else if (! FASTER && "tif".equals(suffix) && pdImage.getColorSpace().equals(PDDeviceGray.INSTANCE)) {
            BufferedImage image = pdImage.getImage();
            if (image == null) {
                return;
            }
            // CCITT compressed images can have a different colorspace, but this one is B/W
            // This is a bitonal image, so copy to TYPE_BYTE_BINARY
            // so that a G4 compressed TIFF image is created by ImageIOUtil.writeImage()
            int w = image.getWidth();
            int h = image.getHeight();
            BufferedImage bitonalImage = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_BINARY);
            // copy image the old fashioned way - ColorConvertOp is slower!
            for (int y = 0; y < h; y++) {
                for (int x = 0; x < w; x++) {
                    bitonalImage.setRGB(x, y, image.getRGB(x, y));
                }
            }
            ImageIOUtil.writeImage(bitonalImage, suffix, out);
        } else if ("jb2".equals(suffix)) {
            InputStream data = pdImage.createInputStream(JB2);
            try {
                copyUpToMaxLength(data, out);
            } finally {
                IOUtils.closeQuietly(data);
            }
        } else {
            if (!FASTER) {
                //this takes forever...if we don't need to do it, don't
                BufferedImage image = pdImage.getImage();
                if (image == null) {
                    return;
                }
                ImageIOUtil.writeImage(image, suffix, out);
            }
        }
        out.flush();
    }

    private static void copyUpToMaxLength(InputStream is, OutputStream os)
            throws IOException, TikaException {
        BoundedInputStream bis = new BoundedInputStream(MAX_IMAGE_LENGTH_BYTES, is);
        IOUtils.copy(bis, os);
        if (bis.hasHitBound()) {
            throw new TikaMemoryLimitException(
                    "Image size is larger than allowed (" + MAX_IMAGE_LENGTH_BYTES + ")");
        }

    }

    private static boolean hasMasks(PDImage pdImage) throws IOException {
        if (pdImage instanceof PDImageXObject) {
            PDImageXObject ximg = (PDImageXObject) pdImage;
            return ximg.getMask() != null || ximg.getSoftMask() != null;
        }
        return false;
    }

    void run() throws IOException {
        PDPage page = getPage();

        //TODO: is there a better way to do this rather than reprocessing the page
        //can we process the text and images in one go?
        processPage(page);
        PDResources res = page.getResources();
        if (res == null) {
            return;
        }

        for (COSName name : res.getExtGStateNames()) {
            PDExtendedGraphicsState extendedGraphicsState = res.getExtGState(name);
            if (extendedGraphicsState != null) {
                PDSoftMask softMask = extendedGraphicsState.getSoftMask();

                if (softMask != null) {
                    try {
                        PDTransparencyGroup group = softMask.getGroup();

                        if (group != null) {
                            // PDFBOX-4327: without this line NPEs will occur
                            res.getExtGState(name).copyIntoGraphicsState(getGraphicsState());

                            processSoftMask(group);
                        }
                    } catch (IOException e) {
                        handleCatchableIOE(e);
                    }
                }
            }
        }
    }

    /**
     * This is the hook for intercepting draw objects so that we can get the COSObjectKeys
     * @param operator
     * @param operands
     * @throws IOException
     */
    @Override
    protected void processOperator(Operator operator, List<COSBase> operands) throws IOException {

        if (operator.getName().equals("Do")) {
            //draw object ... process
            COSObjectKey k = recordDrawObject(operands);
            COSObjectKey prevKey = currObjectKey;
            currObjectKey = k;
            super.processOperator(operator, operands);
            currObjectKey = prevKey;
        }
    }

    private COSObjectKey recordDrawObject(List<COSBase> operands) throws IOException {
        if (operands.isEmpty()) {
            return null;
        } else {
            COSBase base0 = (COSBase)operands.get(0);
            if (base0 instanceof COSName) {
                COSName objectName = (COSName) base0;
                COSDictionary cosDictionary = getResources().getCOSObject();
                COSBase b = cosDictionary.getItem(COSName.XOBJECT);
                if (b != null && b instanceof COSDictionary) {
                    COSObject obj = ((COSDictionary)b).getCOSObject(objectName);
                    return new COSObjectKey(obj);
                }
            }
        }
        return null;
    }


    @Override
    public void drawImage(PDImage pdImage) throws IOException {
        COSBase cosPDImage = pdImage.getCOSObject();

        Metadata metadata = new Metadata();

        if (cosPDImage instanceof COSStream) {
            if (processedStreams.contains((COSStream) cosPDImage)) {
                return;
            }
            processedStreams.add((COSStream) cosPDImage);
            String filterChain = PDFSpelunker.getFilterString(
                    PDFSpelunker.getFilters(((COSStream) cosPDImage)));
            metadata.set(PDFSpelunker.STREAM_FILTER_CHAIN, filterChain);
        }
        metadata.set(PDFSpelunker.STREAM_TYPE, "XObject");
        String cosPath = "";
        if (currObjectKey != null) {
            cosPath = parseState.getCosPath(currObjectKey);
            metadata.set(PDFSpelunker.COS_PATH, cosPath);
        }
        int imageNumber = 0;
        if (pdImage instanceof PDImageXObject) {
            PDImageXObject softMask = ((PDImageXObject) pdImage).getSoftMask();
            if (softMask != null) {
                //we are currently suppressing treating softmasks
                //as their own images.
                processedStreams.add(softMask.getCOSObject());
            }
        }
        if (pdImage instanceof PDImageXObject) {
            if (pdImage.isStencil()) {
                processColor(getGraphicsState().getNonStrokingColor());
            }
            imageNumber = imageCounter.getAndIncrement();

        } else {
            imageNumber = imageCounter.getAndIncrement();
        }
        //TODO: should we use the hash of the PDImage to check for seen
        //For now, we're relying on the cosobject, but this could lead to
        //duplicates if the pdImage is not a PDImageXObject?
        try {
            processImage(pdImage, metadata, imageNumber);
        } catch (TikaException | SAXException e) {
            throw new IOException(e);
        } catch (IOException e) {
            e.printStackTrace();
            handleCatchableIOE(e);
        }
    }

    @Override
    public void appendRectangle(Point2D p0, Point2D p1, Point2D p2, Point2D p3) throws IOException {

    }

    @Override
    public void clip(int windingRule) throws IOException {

    }

    @Override
    public void moveTo(float x, float y) throws IOException {

    }

    @Override
    public void lineTo(float x, float y) throws IOException {

    }

    @Override
    public void curveTo(float x1, float y1, float x2, float y2, float x3, float y3)
            throws IOException {

    }

    @Override
    public Point2D getCurrentPoint() throws IOException {
        return new Point2D.Float(0, 0);
    }

    @Override
    public void closePath() throws IOException {

    }

    @Override
    public void endPath() throws IOException {

    }

    @Override
    protected void showGlyph(Matrix textRenderingMatrix, PDFont font, int code, String unicode,
                             Vector displacement) throws IOException {

        RenderingMode renderingMode = getGraphicsState().getTextState().getRenderingMode();
        if (renderingMode.isFill()) {
            processColor(getGraphicsState().getNonStrokingColor());
        }

        if (renderingMode.isStroke()) {
            processColor(getGraphicsState().getStrokingColor());
        }
    }

    @Override
    public void strokePath() throws IOException {
        processColor(getGraphicsState().getStrokingColor());
    }

    @Override
    public void fillPath(int windingRule) throws IOException {
        processColor(getGraphicsState().getNonStrokingColor());
    }

    @Override
    public void fillAndStrokePath(int windingRule) throws IOException {
        processColor(getGraphicsState().getNonStrokingColor());
    }

    @Override
    public void shadingFill(COSName shadingName) throws IOException {

    }

    // find out if it is a tiling pattern, then process that one
    private void processColor(PDColor color) throws IOException {
        if (color.getColorSpace() instanceof PDPattern) {
            PDPattern pattern = (PDPattern) color.getColorSpace();
            PDAbstractPattern abstractPattern = pattern.getPattern(color);

            if (abstractPattern instanceof PDTilingPattern) {
                processTilingPattern((PDTilingPattern) abstractPattern, null, null);
            }
        }
    }

    private void processImage(PDImage pdImage, Metadata metadata, int imageNumber)
            throws IOException, TikaException, SAXException {
        //this is the metadata for this particular image
        String suffix = getSuffix(pdImage, metadata);
        String fileName = "image" + imageNumber + "." + suffix;


        AttributesImpl attr = new AttributesImpl();
        attr.addAttribute("", "src", "src", "CDATA", "embedded:" + fileName);
        attr.addAttribute("", "alt", "alt", "CDATA", fileName);
        xhtml.startElement("img", attr);
        xhtml.endElement("img");


        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, fileName);
        metadata.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE,
                TikaCoreProperties.EmbeddedResourceType.INLINE.toString());


        if (embeddedDocumentExtractor.shouldParseEmbedded(metadata)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            if (pdImage instanceof PDImageXObject) {
                //PDMetadataExtractor.extract(((PDImageXObject) pdImage).getMetadata(), metadata,
                // parseContext);
            }
            //extract the metadata contained outside of the image
            try {
                writeToBuffer(pdImage, suffix, useDirectJPEG, buffer);
            } catch (MissingImageReaderException e) {
                EmbeddedDocumentUtil.recordException(e, parentMetadata);
                return;
            } catch (IOException e) {
                EmbeddedDocumentUtil.recordEmbeddedStreamException(e, metadata);
                return;
            }
            try (InputStream embeddedIs = TikaInputStream.get(buffer.toByteArray())) {
                embeddedDocumentExtractor.parseEmbedded(embeddedIs,
                        new EmbeddedContentHandler(xhtml), metadata, false);
            }
        }

    }

    private void extractInlineImageMetadataOnly(PDImage pdImage, Metadata metadata)
            throws IOException, SAXException {
        if (pdImage instanceof PDImageXObject) {
            //PDMetadataExtractor
            //      .extract(((PDImageXObject) pdImage).getMetadata(), metadata, parseContext);
        }
        metadata.set(Metadata.IMAGE_WIDTH, pdImage.getWidth());
        metadata.set(Metadata.IMAGE_LENGTH, pdImage.getHeight());
        //TODO: what else can we extract from the PDImage without rendering?
        ZeroByteFileException.IgnoreZeroByteFileException before =
                parseContext.get(ZeroByteFileException.IgnoreZeroByteFileException.class);
        try {
            parseContext.set(ZeroByteFileException.IgnoreZeroByteFileException.class,
                    ZeroByteFileException.IGNORE_ZERO_BYTE_FILE_EXCEPTION);
            embeddedDocumentExtractor.parseEmbedded(TikaInputStream.get(new byte[0]),
                    new EmbeddedContentHandler(xhtml), metadata, false);
        } finally {
            //replace whatever was there before
            parseContext.set(ZeroByteFileException.IgnoreZeroByteFileException.class, before);
        }
    }

    private String getSuffix(PDImage pdImage, Metadata metadata) throws IOException {
        String suffix = pdImage.getSuffix();

        if (suffix == null || suffix.equals("png")) {
            metadata.set(Metadata.CONTENT_TYPE, "image/png");
            suffix = "png";
        } else if (suffix.equals("jpg")) {
            metadata.set(Metadata.CONTENT_TYPE, "image/jpeg");
        } else if (suffix.equals("tiff")) {
            metadata.set(Metadata.CONTENT_TYPE, "image/tiff");
            suffix = "tif";
        } else if (suffix.equals("jpx")) {
            metadata.set(Metadata.CONTENT_TYPE, "image/jp2");
            // use jp2 suffix for file because jpx not known by windows
            suffix = "jp2";
        } else if (suffix.equals("jb2")) {
            //PDFBox resets suffix to png when image's suffix == jb2
            metadata.set(Metadata.CONTENT_TYPE, "image/x-jbig2");
        } else {
            //TODO: determine if we need to add more image types
//                    throw new RuntimeException("EXTEN:" + extension);
        }
        if (hasMasks(pdImage)) {
            // TIKA-3040, PDFBOX-4771: can't save ARGB as JPEG
            suffix = "png";
        }
        return suffix;
    }

    void handleCatchableIOE(IOException e) throws IOException {
        if (e.getCause() instanceof SAXException && e.getCause().getMessage() != null &&
                e.getCause().getMessage().contains("Your document contained more than")) {
            //TODO -- is there a cleaner way of checking for:
            // WriteOutContentHandler.WriteLimitReachedException?
            throw e;
        }

        String msg = e.getMessage();
        if (msg == null) {
            msg = "IOException, no message";
        }
        parentMetadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_WARNING, msg);
        exceptions.add(e);
    }

    List<IOException> getExceptions() {
        return exceptions;
    }
}
