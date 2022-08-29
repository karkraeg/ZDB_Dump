<xsl:stylesheet version="1.0" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
    
    <!-- keep comments -->
    <xsl:template match="comment()">
        <xsl:copy>
            <xsl:apply-templates/>
        </xsl:copy>
    </xsl:template>
    
    <xsl:template match="/rdf:Description">
        <!-- remove element prefix -->
        <xsl:element name="{local-name()}">
            <!-- process attributes -->
            <xsl:for-each select="@*">
                <!-- remove attribute prefix -->
                <xsl:attribute name="{local-name()}">
                    <xsl:value-of select="."/>
                </xsl:attribute>
            </xsl:for-each>
            
        </xsl:element>
    </xsl:template>
    
    <!-- Kopiert alles, was nicht durch andere Templates bearbeitet wird -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>
    
</xsl:stylesheet>