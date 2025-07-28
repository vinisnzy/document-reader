package com.vinisnzy.document_reader.service;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.awt.image.BufferedImage;
import java.io.IOException;

@Service
public class DocumentReaderService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public DocumentReaderService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "document-order", groupId = "document_reader_group")
    public void readPdf(byte[] pdfData) {
        try {
            PDDocument document = PDDocument.load(pdfData);

            Tesseract tesseract = new Tesseract();
            tesseract.setDatapath("C:/Arquivos de Programas/Tesseract-OCR/tessdata");
            tesseract.setLanguage("por");

            PDFRenderer renderer = new PDFRenderer(document);
            StringBuilder result = new StringBuilder();

            for (int i = 0; i < document.getNumberOfPages(); i++) {
                BufferedImage image = renderer.renderImageWithDPI(i, 300, ImageType.RGB);
                String pageText = tesseract.doOCR(image);

                result.append("Page ").append(i + 1).append(":\n");
                result.append(pageText).append("\n\n");
            }

            document.close();
            kafkaTemplate.send("document-reader", result.toString());
        } catch (IOException | TesseractException e) {
            throw new IllegalArgumentException("Failed to read PDF data", e);
        }
    }
}
